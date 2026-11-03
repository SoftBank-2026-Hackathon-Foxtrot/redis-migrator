package main

// Subscribed sessions. The proxy keeps what each client subscribed to, so a SWITCH moves the
// subscription to the new backend instead of closing the client's connection:
//
//	PAUSE         PUBLISH and the (un)subscribe commands are held at the gate like writes, so the
//	              set of subscriptions stops changing and nothing new is published anywhere
//	PRESUBSCRIBE  each session opens a second upstream on the next backend and subscribes it to
//	              the same channels and patterns. Any failure refuses the cutover before anything
//	              has changed, so the client stays on the old backend, connected
//	SWITCH        a PING with a unique token goes down the old upstream. Redis answers it after
//	              every message already queued for that connection, so once the answer is read,
//	              everything the old backend published has been relayed. The new upstream then
//	              takes over and the old one is closed
//	RESUME        the held PUBLISHes go to the new backend, where the subscriptions already are
//
// A message is published to exactly one backend, so none arrives twice. MONITOR and SSUBSCRIBE are
// not moved: those sessions are still closed on a switch. A moved session only takes the commands
// in subSessionCmds: anything else would need the gate, the checks and the tracking of session.run.

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"statemig/internal/redisx"
)

var (
	movableSubCmds = set("subscribe", "psubscribe")
	subSessionCmds = set("subscribe", "psubscribe", "unsubscribe", "punsubscribe", "ping")
)

const (
	clientWriteTimeout = 5 * time.Second        // a subscriber that takes no data for this long is closed
	stuckClientGrace   = 100 * time.Millisecond // a reader whose upstream is closed can only be stuck on its client
)

var errStuckClient = errors.New("client is not reading")

// subState is what a subscribed session needs to be moved.
type subState struct {
	mu          sync.Mutex
	rd          *subReader // reads the upstream messages come from
	channels    map[string]bool
	patterns    map[string]bool
	prepared    *redisx.Conn // subscribed on the next backend, not read yet
	preparedFor string
}

// subReader relays every frame of one upstream to the client, until a barrier answer or an error.
type subReader struct {
	conn    *redisx.Conn
	ep      *redisx.Endpoint
	barrier atomic.Pointer[[2][]byte] // the PING answer to stop at, RESP2 and RESP3 form
	reached chan struct{}             // closed when the barrier answer was read
	retired atomic.Bool               // a switch replaced this upstream: errors are expected
	done    chan struct{}
}

// track applies a (un)subscribe the client sends, when it is sent: a PRESUBSCRIBE must cover a
// subscription whose confirmation has not come back yet.
func (st *subState) track(name string, args [][]byte) {
	set := st.channels
	if strings.HasPrefix(name, "p") {
		set = st.patterns
	}
	switch name {
	case "subscribe", "psubscribe":
		for _, a := range args[1:] {
			set[string(a)] = true
		}
	case "unsubscribe", "punsubscribe":
		if len(args) == 1 {
			clear(set)
		}
		for _, a := range args[1:] {
			delete(set, string(a))
		}
	}
}

// dialUp opens an upstream on the session's db and protocol (HELLO is replayed).
func (s *session) dialUp(ep *redisx.Endpoint, timeout time.Duration) (*redisx.Conn, error) {
	c, err := ep.Dial(s.db)
	if err != nil {
		return nil, err
	}
	c.NetConn().SetDeadline(time.Now().Add(timeout))
	if s.hello != nil {
		redisx.WriteCommand(c.Writer(), s.hello...)
		if err = c.Writer().Flush(); err == nil {
			_, err = redisx.ReadRawReply(c.Reader(), nil)
		}
		if err != nil {
			c.Close()
			return nil, err
		}
	}
	return c, nil
}

// subscribeAll subscribes c to the session's channels and patterns and reads every confirmation.
func (st *subState) subscribeAll(c *redisx.Conn) error {
	n := 0
	for _, cmd := range []struct {
		name string
		set  map[string]bool
	}{{"SUBSCRIBE", st.channels}, {"PSUBSCRIBE", st.patterns}} {
		if len(cmd.set) == 0 {
			continue
		}
		args := [][]byte{[]byte(cmd.name)}
		for k := range cmd.set {
			args = append(args, []byte(k))
		}
		redisx.WriteCommand(c.Writer(), args...)
		n += len(cmd.set)
	}
	if err := c.Writer().Flush(); err != nil {
		return err
	}
	for i := 0; i < n; i++ {
		frame, err := redisx.ReadRawReply(c.Reader(), nil)
		if err != nil {
			return err
		}
		if len(frame) > 0 && frame[0] == '-' {
			return errors.New(strings.TrimSpace(string(frame[1:])))
		}
	}
	return nil
}

// writeClient sends one frame to the client; the reader and the command loop both write.
func (s *session) writeClient(frame []byte) error {
	s.wmu.Lock()
	defer s.wmu.Unlock()
	s.nc.SetWriteDeadline(time.Now().Add(clientWriteTimeout))
	s.cw.Write(frame)
	return s.cw.Flush()
}

func (s *session) startReader(st *subState, c *redisx.Conn, ep *redisx.Endpoint) *subReader {
	rd := &subReader{conn: c, ep: ep, reached: make(chan struct{}), done: make(chan struct{})}
	st.rd = rd
	go func() {
		defer close(rd.done)
		c.NetConn().SetDeadline(time.Time{})
		for {
			frame, err := redisx.ReadRawReply(c.Reader(), nil)
			if err != nil {
				if !rd.retired.Load() {
					go s.repair(st, rd) // the backend went away under a live subscription
				}
				return
			}
			if b := rd.barrier.Load(); b != nil && (bytes.Equal(frame, b[0]) || bytes.Equal(frame, b[1])) {
				close(rd.reached)
				return
			}
			if s.writeClient(frame) != nil {
				s.nc.Close()
				return
			}
		}
	}()
	return rd
}

// repair re-subscribes on the active backend after the upstream died, retrying while a failure
// rollback puts the route back; the client stays connected. Messages published to the dead
// backend before it went are lost: Pub/Sub keeps nothing. Gives up (closes the client) after a while.
func (s *session) repair(st *subState, dead *subReader) {
	for end := time.Now().Add(repairFor); time.Now().Before(end); time.Sleep(250 * time.Millisecond) {
		if s.sub.Load() == nil {
			return
		}
		st.mu.Lock()
		moved := st.rd != dead // a switch moved it meanwhile
		st.mu.Unlock()
		if moved {
			return
		}
		route, _ := s.p.Route()
		c, err := s.dialUp(route.Active, time.Second) // not under st.mu: a switch must not wait on this
		if err != nil {
			continue
		}
		st.mu.Lock()
		if st.rd != dead {
			st.mu.Unlock()
			c.Close()
			return
		}
		if err := st.subscribeAll(c); err != nil {
			st.mu.Unlock()
			c.Close()
			continue
		}
		dead.conn.Close()
		s.startReader(st, c, route.Active)
		st.mu.Unlock()
		s.p.Stats.PubsubRepaired.Add(1)
		log.Printf("pubsub session re-subscribed on %s after its upstream died", route.Active.Name)
		return
	}
	s.nc.Close()
}

const repairFor = 15 * time.Second

// subscribed runs a session from its first SUBSCRIBE/PSUBSCRIBE on. ep is where that went.
func (s *session) subscribed(ep *redisx.Endpoint, name string, args [][]byte) {
	p := s.p
	st := &subState{channels: map[string]bool{}, patterns: map[string]bool{}}
	st.track(name, args)
	st.mu.Lock()
	up := s.ups[ep.Name]
	delete(s.ups, ep.Name) // owned by the subscription from here on
	s.wmu.Lock()
	s.cw.Flush()
	s.wmu.Unlock()
	s.startReader(st, up, ep)
	st.mu.Unlock()
	s.sub.Store(st)
	defer func() {
		s.sub.Store(nil)
		st.mu.Lock()
		st.rd.retired.Store(true)
		st.rd.conn.Close()
		if st.prepared != nil {
			st.prepared.Close()
		}
		st.mu.Unlock()
	}()
	for {
		args, err := redisx.ReadCommand(s.cr)
		if err != nil {
			return
		}
		name := string(bytes.ToLower(args[0]))
		if name == "quit" {
			s.writeClient(redisx.SimpleReply("OK"))
			return
		}
		why := ""
		if !subSessionCmds[name] {
			why = "ERR " + name + " is not allowed on a subscribed connection through the proxy"
		} else if d := s.role.deny(name, args, p.keys); d != "" {
			why = "NOPERM " + d
		}
		if why != "" {
			p.Stats.Denied.Add(1)
			if s.writeClient(redisx.ErrorReply(why)) != nil {
				return
			}
			continue
		}
		_, _, held, _, pending := p.enter(name, args, s.db, false, s.resp3())
		st.mu.Lock()
		st.track(name, args)
		st.rd.conn.NetConn().SetWriteDeadline(time.Now().Add(p.upstreamTimeout)) // a switch waits on st.mu
		redisx.WriteCommand(st.rd.conn.Writer(), args...)
		err = st.rd.conn.Writer().Flush()
		st.mu.Unlock()
		p.exit(held, 0, pending)
		if err != nil {
			return
		}
	}
}

// prepare subscribes a second upstream on ep. Called while paused, so the subscriptions are fixed.
func (s *session) prepare(st *subState, ep *redisx.Endpoint, timeout time.Duration) error {
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.rd.ep == ep {
		return nil
	}
	if st.prepared != nil && st.preparedFor == ep.Name {
		return nil
	}
	if st.prepared != nil {
		st.prepared.Close()
		st.prepared = nil
	}
	c, err := s.dialUp(ep, timeout)
	if err != nil {
		return err
	}
	if err := st.subscribeAll(c); err != nil {
		c.Close()
		return err
	}
	st.prepared, st.preparedFor = c, ep.Name
	return nil
}

func token() string {
	b := make([]byte, 12)
	rand.Read(b)
	return "statemig-barrier-" + hex.EncodeToString(b)
}

// move hands the subscription over to ep: barrier on the old upstream, then the new one reads.
// It returns whether every message of the old upstream was relayed first.
func (s *session) move(st *subState, ep *redisx.Endpoint, timeout time.Duration) (bool, error) {
	if err := s.prepare(st, ep, timeout); err != nil {
		return false, err
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	old := st.rd
	if old.ep == ep {
		return true, nil
	}
	tok := token()
	old.barrier.Store(&[2][]byte{
		[]byte("*2\r\n$4\r\npong\r\n$" + strconv.Itoa(len(tok)) + "\r\n" + tok + "\r\n"), // RESP2, subscribed
		[]byte("$" + strconv.Itoa(len(tok)) + "\r\n" + tok + "\r\n"),                     // RESP3
	})
	complete := false
	old.conn.NetConn().SetWriteDeadline(time.Now().Add(timeout))
	redisx.WriteCommand(old.conn.Writer(), []byte("PING"), []byte(tok))
	if old.conn.Writer().Flush() == nil {
		select {
		case <-old.reached:
			complete = true
		case <-old.done: // the old backend is gone (a failure rollback): nothing more can come from it
		case <-time.After(timeout):
		}
	}
	old.retired.Store(true)
	old.conn.Close()
	c := st.prepared
	st.prepared, st.preparedFor = nil, ""
	select {
	case <-old.done:
	case <-time.After(stuckClientGrace): // writes are held for every client while this one waits
		s.nc.Close()
		<-old.done
		c.Close()
		return false, errStuckClient
	}
	s.startReader(st, c, ep)
	return complete, nil
}

// subSessions lists the sessions that hold a movable subscription.
func (p *Proxy) subSessions() []*session {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []*session
	for s := range p.sessions {
		if s.sub.Load() != nil {
			out = append(out, s)
		}
	}
	return out
}

// eachSub runs fn for every subscribed session at once and joins the errors.
func (p *Proxy) eachSub(fn func(*session, *subState) error) (int, error) {
	subs := p.subSessions()
	var wg sync.WaitGroup
	errs := make([]error, len(subs))
	for i, s := range subs {
		wg.Add(1)
		go func(i int, s *session) {
			defer wg.Done()
			if st := s.sub.Load(); st != nil { // nil: the session ended meanwhile
				errs[i] = fn(s, st)
			}
		}(i, s)
	}
	wg.Wait()
	return len(subs), errors.Join(errs...)
}

// Presubscribe prepares every subscribed session on ep. Only while paused; nothing changes on error.
func (p *Proxy) Presubscribe(ep *redisx.Endpoint, timeout time.Duration) (int, error) {
	if _, paused := p.Route(); !paused {
		return 0, fmt.Errorf("%w: presubscribe requires PAUSE", ErrState)
	}
	n, err := p.eachSub(func(s *session, st *subState) error { return s.prepare(st, ep, timeout) })
	if err != nil {
		p.dropPrepared("")
		return n, fmt.Errorf("%w: presubscribe on %s: %v", ErrState, ep.Name, err)
	}
	return n, nil
}

// dropPrepared closes prepared upstreams that are not for keep (a cutover that was called off).
func (p *Proxy) dropPrepared(keep string) {
	for _, s := range p.subSessions() {
		st := s.sub.Load()
		if st == nil {
			continue
		}
		st.mu.Lock()
		if st.prepared != nil && st.preparedFor != keep {
			st.prepared.Close()
			st.prepared, st.preparedFor = nil, ""
		}
		st.mu.Unlock()
	}
}

// moveSubs moves every subscription to the active backend. A session that cannot be moved is
// closed, as before, so its client resubscribes; a normal cutover has prepared them all already.
func (p *Proxy) moveSubs(ep *redisx.Endpoint) {
	p.eachSub(func(s *session, st *subState) error {
		complete, err := s.move(st, ep, time.Second)
		switch {
		case err != nil:
			p.Stats.PubsubDropped.Add(1)
			log.Printf("pubsub session not moved to %s, closing it: %v", ep.Name, err)
			s.nc.Close()
		case complete:
			p.Stats.PubsubMoved.Add(1)
		default:
			p.Stats.PubsubCut.Add(1) // moved, but the old backend's last messages could not be confirmed
		}
		return nil
	})
}
