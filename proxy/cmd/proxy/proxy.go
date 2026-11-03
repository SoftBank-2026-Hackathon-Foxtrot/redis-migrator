package main

// Central RESP proxy. Every app talks to this one process at a fixed address.
//
//  1. Every command goes to the *active* backend, except queue consumer
//     commands, which go to the *consume* backend (the old one until its queue
//     is drained).
//  2. PAUSE holds write commands (and new MULTI/WATCH) at the gate; reads keep
//     flowing. It completes once no held-type command is in flight and no
//     transaction is open.
//  3. SWITCH changes the route inside this one process, so there is no state
//     where part of the fleet writes to the old Redis. RESUME releases the
//     held commands to the new route.

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"statemig/internal/redisx"
)

// Route is the proxy's view of the control record for one generation.
type Route struct {
	Gen     uint64
	Active  *redisx.Endpoint // reads, writes, produce
	Consume *redisx.Endpoint // queue consumer commands
}

// Client is one account allowed to connect to the proxy.
type Client struct {
	Password string
	Role     Role
}

type Proxy struct {
	clients map[string]Client // empty: no auth, no role limits
	cmds    CmdTable
	keys    KeyTable

	mu       sync.Mutex
	cond     *sync.Cond
	route    Route
	epoch    uint64 // bumped on every route change; sessions drop stale upstreams
	paused   bool
	pausedAt time.Time
	inflight int // held-type commands currently at a backend
	tx       int // sessions inside MULTI/WATCH
	sessions map[*session]struct{}
	blockCap int // ms; caps XREADGROUP BLOCK while consume != active
	// cache keys with a write held at the gate and not finished yet, with how many
	pendingCache map[string]int

	upstreamTimeout time.Duration // per-command I/O deadline towards a backend

	Stats   ProxyStats
	latency *latencyWindow
}

type ProxyStats struct {
	Commands       atomic.Int64
	HeldCommands   atomic.Int64 // commands that actually waited at the gate
	ErrorReplies   atomic.Int64
	UpstreamErrors atomic.Int64
	Denied         atomic.Int64
	Switches       atomic.Int64
	CacheBypassed  atomic.Int64 // cache commands answered by the proxy during PAUSE
	LastPauseMs   atomic.Value // float64, PAUSE → RESUME
	MaxPauseMs     atomic.Value
}

// loadTables reads the command table from the first backend that answers: a dead
// active backend must not keep the proxy down, or nothing could roll back from it.
func loadTables(eps []*redisx.Endpoint) (CmdTable, KeyTable, error) {
	var err error
	for _, ep := range eps {
		var c *redisx.Conn
		if c, err = ep.Dial(0); err != nil {
			err = fmt.Errorf("connect %s: %w", ep.Name, err)
			continue
		}
		ct, kt, e := LoadCmdTable(c)
		c.Close()
		if e == nil {
			return ct, kt, nil
		}
		err = fmt.Errorf("COMMAND on %s: %w", ep.Name, e)
	}
	return nil, nil, err
}

func NewProxy(route Route, paused bool, clients map[string]Client, backends []*redisx.Endpoint) (*Proxy, error) {
	ct, kt, err := loadTables(backends)
	if err != nil {
		return nil, err
	}
	p := &Proxy{clients: clients, cmds: ct, keys: kt, route: route, paused: paused, sessions: map[*session]struct{}{},
		pendingCache: map[string]int{},
		latency: newLatencyWindow(20000), blockCap: 200, upstreamTimeout: 3 * time.Second}
	if paused {
		p.pausedAt = time.Now()
	}
	p.cond = sync.NewCond(&p.mu)
	p.Stats.LastPauseMs.Store(0.0)
	p.Stats.MaxPauseMs.Store(0.0)
	return p, nil
}

func (p *Proxy) Serve(ln net.Listener) error {
	for {
		nc, err := ln.Accept()
		if err != nil {
			return err
		}
		s := &session{p: p, nc: nc, cr: bufio.NewReaderSize(nc, 64<<10), cw: bufio.NewWriterSize(nc, 64<<10),
			authed: len(p.clients) == 0, ups: map[string]*redisx.Conn{}}
		p.mu.Lock()
		p.sessions[s] = struct{}{}
		p.mu.Unlock()
		go s.run()
	}
}

func (p *Proxy) Route() (Route, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.route, p.paused
}

// enter waits at the gate for held-type commands while paused and counts them
// as in flight. Commands inside an open transaction pass, so it can finish.
// Queue consumer commands are neither held nor counted: SWITCH does not move
// them (they stay on the consume backend until DRAIN_DONE), and a blocking
// XREADGROUP would otherwise stretch every PAUSE up to its BLOCK timeout.
// A cache command around a PAUSE may be answered here (the last result), under the same
// lock, so a fill can't slip past the check and end up held instead of dropped. The third
// last result lists the cache keys this command holds as pending; pass it back to exit.
func (p *Proxy) enter(name string, args [][]byte, inTx, resp3 bool) (Route, uint64, bool, []byte, []string) {
	held := !inTx && !consumeCmds[name] && p.cmds.Held(name)
	p.mu.Lock()
	if !inTx && (p.paused || len(p.pendingCache) > 0) {
		if reply := cacheReply(name, args, resp3, p.paused, p.pendingCache); reply != nil {
			p.mu.Unlock()
			return Route{}, 0, false, reply, nil
		}
	}
	var pending []string
	if held && p.paused {
		for _, k := range p.keys.keysOf(name, args) {
			if isCacheKey(k) {
				pending = append(pending, string(k))
				p.pendingCache[string(k)]++
			}
		}
		p.Stats.HeldCommands.Add(1)
		for p.paused {
			p.cond.Wait()
		}
	}
	if held {
		p.inflight++
	}
	r, e := p.route, p.epoch
	p.mu.Unlock()
	return r, e, held, nil, pending
}

func (p *Proxy) exit(held bool, txDelta int, pending []string) {
	p.mu.Lock()
	if held {
		p.inflight--
	}
	for _, k := range pending {
		if p.pendingCache[k]--; p.pendingCache[k] <= 0 {
			delete(p.pendingCache, k)
		}
	}
	p.tx += txDelta
	if p.paused {
		p.cond.Broadcast() // Pause may be waiting for inflight/tx to reach 0
	}
	p.mu.Unlock()
}

var ErrState = errors.New("bad state")

// Pause closes the gate for writes and waits until no write and no transaction
// is in flight. On timeout the gate stays closed; the caller decides to resume.
func (p *Proxy) Pause(maxWait time.Duration) (float64, error) {
	t0 := time.Now()
	deadline := t0.Add(maxWait)
	p.mu.Lock()
	if !p.paused {
		p.paused = true
		p.pausedAt = t0
	}
	p.mu.Unlock()
	for {
		p.mu.Lock()
		inflight, tx := p.inflight, p.tx
		p.mu.Unlock()
		if inflight == 0 && tx == 0 {
			return msSince(t0), nil
		}
		if time.Now().After(deadline) {
			return msSince(t0), fmt.Errorf("%w: quiesce timeout (inflight=%d, tx=%d)", ErrState, inflight, tx)
		}
		time.Sleep(200 * time.Microsecond)
	}
}

// Switch changes the route. Only allowed while paused and only to a newer generation.
func (p *Proxy) Switch(r Route) error {
	p.mu.Lock()
	if !p.paused {
		p.mu.Unlock()
		return fmt.Errorf("%w: switch requires PAUSE", ErrState)
	}
	if r.Gen <= p.route.Gen {
		p.mu.Unlock()
		return fmt.Errorf("%w: gen %d is not newer than %d", ErrState, r.Gen, p.route.Gen)
	}
	old := p.route
	p.route = r
	p.epoch++
	var subs []*session
	for s := range p.sessions {
		if s.pubsub.Load() { // subscriptions live on the old server: make clients resubscribe
			subs = append(subs, s)
		}
	}
	p.mu.Unlock()
	for _, s := range subs {
		s.nc.Close()
	}
	p.Stats.Switches.Add(1)
	log.Printf("switch gen %d→%d active %s→%s consume %s→%s", old.Gen, r.Gen, old.Active.Name, r.Active.Name,
		old.Consume.Name, r.Consume.Name)
	return nil
}

// DrainDone moves queue consumers to a new backend within the same generation.
func (p *Proxy) DrainDone(gen uint64, consume *redisx.Endpoint) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if gen != p.route.Gen {
		return fmt.Errorf("%w: gen %d, proxy is at %d", ErrState, gen, p.route.Gen)
	}
	log.Printf("drain done gen %d: consume %s→%s", gen, p.route.Consume.Name, consume.Name)
	p.route.Consume = consume
	p.epoch++
	return nil
}

func (p *Proxy) Resume() float64 {
	p.mu.Lock()
	if !p.paused {
		p.mu.Unlock()
		return 0
	}
	p.paused = false
	ms := msSince(p.pausedAt)
	p.cond.Broadcast()
	p.mu.Unlock()
	p.Stats.LastPauseMs.Store(ms)
	if ms > p.Stats.MaxPauseMs.Load().(float64) {
		p.Stats.MaxPauseMs.Store(ms)
	}
	log.Printf("resume (paused %.2fms)", ms)
	return ms
}

func (p *Proxy) Status() map[string]any {
	p.mu.Lock()
	st := map[string]any{
		"gen": p.route.Gen, "active": p.route.Active.Name, "consume": p.route.Consume.Name,
		"paused": p.paused, "inflight_writes": p.inflight, "open_tx": p.tx, "clients": len(p.sessions),
	}
	p.mu.Unlock()
	st["commands"] = p.Stats.Commands.Load()
	st["held_commands"] = p.Stats.HeldCommands.Load()
	st["error_replies"] = p.Stats.ErrorReplies.Load()
	st["upstream_errors"] = p.Stats.UpstreamErrors.Load()
	st["denied"] = p.Stats.Denied.Load()
	st["switches"] = p.Stats.Switches.Load()
	st["cache_bypassed"] = p.Stats.CacheBypassed.Load()
	st["last_pause_ms"] = p.Stats.LastPauseMs.Load()
	st["max_pause_ms"] = p.Stats.MaxPauseMs.Load()
	st["latency_ms"] = p.latency.summary()
	return st
}

func msSince(t time.Time) float64 { return float64(time.Since(t).Microseconds()) / 1000 }

// ---- session -----------------------------------------------------------------

type session struct {
	p      *Proxy
	nc     net.Conn
	cr     *bufio.Reader
	cw     *bufio.Writer
	authed bool
	role   Role

	db       int
	ups      map[string]*redisx.Conn // one upstream per backend
	upEpoch  uint64
	multi    bool
	watching bool
	txEP     *redisx.Endpoint // a transaction stays on the backend it started on
	hello    [][]byte         // HELLO (protocol version, SETNAME) replayed on reconnect
	pubsub   atomic.Bool
}

// login checks an account and remembers its role.
func (s *session) login(user, password string) bool {
	if len(s.p.clients) == 0 {
		s.authed = true
		return true
	}
	c, ok := s.p.clients[user]
	if !ok || c.Password != password {
		return false
	}
	s.authed, s.role = true, c.Role
	return true
}

func (s *session) stripHelloAuth(args [][]byte) ([][]byte, bool) {
	out := [][]byte{args[0]}
	for i := 1; i < len(args); i++ {
		if strings.EqualFold(string(args[i]), "auth") && i+2 < len(args) {
			if !s.login(string(args[i+1]), string(args[i+2])) {
				return nil, false
			}
			i += 2
			continue
		}
		out = append(out, args[i])
	}
	return out, true
}

func (s *session) inTx() bool { return s.multi || s.watching }

func (s *session) resp3() bool { return len(s.hello) > 1 && string(s.hello[1]) == "3" }

func (s *session) close() {
	p := s.p
	p.mu.Lock()
	delete(p.sessions, s)
	if s.inTx() {
		p.tx--
		if p.paused {
			p.cond.Broadcast()
		}
	}
	p.mu.Unlock()
	s.multi, s.watching = false, false
	for _, c := range s.ups {
		c.Close()
	}
	s.nc.Close()
}

func (s *session) reply(r []byte) { s.cw.Write(r) }

func (s *session) run() {
	defer s.close()
	p := s.p
	buf := make([]byte, 0, 4096)
	for {
		if s.cr.Buffered() == 0 { // flush once per pipeline batch
			if err := s.cw.Flush(); err != nil {
				return
			}
		}
		args, err := redisx.ReadCommand(s.cr)
		if err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
				s.cw.Write(redisx.ErrorReply("ERR " + err.Error()))
				s.cw.Flush()
			}
			return
		}
		name := string(bytes.ToLower(args[0]))

		// answered by the proxy itself
		switch {
		case name == "auth":
			user := "default"
			if len(args) >= 3 {
				user = string(args[1])
			}
			if s.login(user, string(args[len(args)-1])) {
				s.reply(redisx.SimpleReply("OK"))
			} else {
				s.reply(redisx.ErrorReply("WRONGPASS invalid username-password pair"))
			}
			continue
		case !s.authed && name != "hello": // HELLO may carry AUTH, checked below
			s.reply(redisx.ErrorReply("NOAUTH Authentication required."))
			continue
		case name == "quit":
			s.reply(redisx.SimpleReply("OK"))
			s.cw.Flush()
			return
		case deniedCmds[name]:
			p.Stats.Denied.Add(1)
			s.reply(redisx.ErrorReply("ERR " + name + " is not allowed through the proxy"))
			continue
		case name == "hello":
			// Backend auth is the proxy's job: check AUTH against the proxy password,
			// strip it, and remember the rest so it's replayed on every new upstream.
			var ok bool
			if args, ok = s.stripHelloAuth(args); !ok {
				s.reply(redisx.ErrorReply("WRONGPASS invalid username-password pair"))
				continue
			}
			if !s.authed {
				s.reply(redisx.ErrorReply("NOAUTH HELLO must be called with the client already authenticated"))
				continue
			}
		}
		if why := s.role.deny(name, args, p.keys); why != "" {
			p.Stats.Denied.Add(1)
			s.reply(redisx.ErrorReply("NOPERM " + why))
			continue
		}

		t0 := time.Now() // includes time at the gate: the pause shows up in latency honestly
		wasTx := s.inTx()
		route, epoch, held, answered, pending := p.enter(name, args, wasTx, s.resp3())
		if answered != nil {
			p.Stats.CacheBypassed.Add(1)
			s.reply(answered)
			continue
		}

		ep := route.Active
		switch {
		case wasTx:
			ep = s.txEP
		case consumeCmds[name]:
			ep = route.Consume
			if name == "xreadgroup" && route.Consume != route.Active {
				capBlock(args, p.blockCap) // blocked readers come back soon and re-route after DRAIN_DONE
			}
		}
		buf, err = s.forward(ep, route, epoch, args, buf[:0])
		if err != nil {
			p.Stats.UpstreamErrors.Add(1)
			buf = append(buf[:0], redisx.ErrorReply("ERR upstream unavailable: "+err.Error())...)
			// A failure rollback pauses because this backend is dead: the cache is optional, the DB answers.
			if _, paused := p.Route(); paused && name == "get" && len(args) > 1 && isCacheKey(args[1]) {
				buf = append(buf[:0], cacheMiss(s.resp3())...)
			}
		}
		s.track(name, args, buf, ep)
		txDelta := 0
		if s.inTx() && !wasTx {
			txDelta = 1
		} else if wasTx && !s.inTx() {
			txDelta = -1
		}
		p.exit(held, txDelta, pending)

		p.Stats.Commands.Add(1)
		p.latency.add(time.Since(t0))
		if len(buf) > 0 && buf[0] == '-' {
			p.Stats.ErrorReplies.Add(1)
		}
		s.reply(buf)

		if pubsubCmds[name] && len(buf) > 0 && buf[0] != '-' {
			s.pubsub.Store(true)
			s.pipe(s.ups[ep.Name])
			return
		}
	}
}

// forward sends one command to ep. After a route change, upstreams to backends
// that are no longer in the route are closed (the old writer connection goes away).
func (s *session) forward(ep *redisx.Endpoint, route Route, epoch uint64, args [][]byte, dst []byte) ([]byte, error) {
	if s.upEpoch != epoch {
		for name, c := range s.ups {
			if name != route.Active.Name && name != route.Consume.Name && (s.txEP == nil || name != s.txEP.Name) {
				c.Close()
				delete(s.ups, name)
			}
		}
		s.upEpoch = epoch
	}
	up := s.ups[ep.Name]
	if up == nil {
		c, err := ep.Dial(s.db)
		if err != nil {
			return dst, err
		}
		if s.hello != nil && !strings.EqualFold(string(args[0]), "hello") {
			redisx.WriteCommand(c.Writer(), s.hello...)
			if err := c.Writer().Flush(); err == nil {
				_, err = redisx.ReadRawReply(c.Reader(), nil)
			}
			if err != nil {
				c.Close()
				return dst, err
			}
		}
		s.ups[ep.Name] = c
		up = c
	}
	// A dead backend must fail the command, not hang it: a hung write would keep
	// PAUSE waiting and the app request open.
	if d := s.p.deadline(args); d > 0 {
		up.NetConn().SetDeadline(time.Now().Add(d))
	} else {
		up.NetConn().SetDeadline(time.Time{})
	}
	redisx.WriteCommand(up.Writer(), args...)
	if err := up.Writer().Flush(); err != nil {
		up.Close()
		delete(s.ups, ep.Name)
		return dst, err
	}
	out, err := redisx.ReadRawReply(up.Reader(), dst)
	if err != nil {
		up.Close()
		delete(s.ups, ep.Name)
	}
	return out, err
}

// deadline is the upstream I/O timeout for one command; 0 = none (BLOCK 0 waits forever).
func (p *Proxy) deadline(args [][]byte) time.Duration {
	ms, blocking := blockMs(args)
	switch {
	case !blocking:
		return p.upstreamTimeout
	case ms == 0:
		return 0
	default:
		return time.Duration(ms)*time.Millisecond + p.upstreamTimeout
	}
}

// blockMs finds a BLOCK argument (XREAD / XREADGROUP) before STREAMS.
func blockMs(args [][]byte) (int, bool) {
	for i := 1; i+1 < len(args); i++ {
		a := strings.ToLower(string(args[i]))
		if a == "streams" {
			break
		}
		if a == "block" {
			n, err := strconv.Atoi(string(args[i+1]))
			return n, err == nil
		}
	}
	return 0, false
}

func (s *session) track(name string, args [][]byte, reply []byte, ep *redisx.Endpoint) {
	isErr := len(reply) > 0 && reply[0] == '-'
	switch {
	case name == "select" && !isErr:
		s.db, _ = strconv.Atoi(string(args[1]))
		for n, c := range s.ups { // other backends' connections are on the old db
			if n != ep.Name {
				c.Close()
				delete(s.ups, n)
			}
		}
	case name == "hello" && !isErr:
		s.hello = args
	case name == "watch" && !isErr:
		s.watching = true
		s.txEP = ep
	case name == "multi" && !isErr:
		if !s.watching {
			s.txEP = ep
		}
		s.multi = true
	case name == "exec" || name == "discard":
		s.multi, s.watching, s.txEP = false, false, nil
	case name == "unwatch" && !s.multi:
		s.watching, s.txEP = false, nil
	}
}

// capBlock lowers an XREADGROUP BLOCK timeout in place.
func capBlock(args [][]byte, capMs int) {
	for i := 1; i+1 < len(args); i++ {
		a := strings.ToLower(string(args[i]))
		if a == "streams" {
			return
		}
		if a == "block" {
			if n, err := strconv.Atoi(string(args[i+1])); err == nil && (n == 0 || n > capMs) {
				args[i+1] = []byte(strconv.Itoa(capMs))
			}
			return
		}
	}
}

// pipe shovels bytes both ways for Pub/Sub / MONITOR until either side closes.
func (s *session) pipe(up *redisx.Conn) {
	s.cw.Flush()
	done := make(chan struct{}, 2)
	go func() { io.Copy(up.NetConn(), s.cr); done <- struct{}{} }()
	go func() { io.Copy(s.nc, up.Reader()); done <- struct{}{} }()
	<-done
}

// ---- latency -------------------------------------------------------------------

type latencyWindow struct {
	mu  sync.Mutex
	buf []float64
	i   int
	n   int
}

func newLatencyWindow(n int) *latencyWindow { return &latencyWindow{buf: make([]float64, n)} }

func (l *latencyWindow) add(d time.Duration) {
	l.mu.Lock()
	l.buf[l.i] = float64(d.Microseconds()) / 1000
	l.i = (l.i + 1) % len(l.buf)
	if l.n < len(l.buf) {
		l.n++
	}
	l.mu.Unlock()
}

func (l *latencyWindow) summary() map[string]float64 {
	l.mu.Lock()
	xs := append([]float64(nil), l.buf[:l.n]...)
	l.mu.Unlock()
	if len(xs) == 0 {
		return map[string]float64{}
	}
	sort.Float64s(xs)
	q := func(f float64) float64 { return xs[min(int(f*float64(len(xs))), len(xs)-1)] }
	return map[string]float64{"p50": q(0.5), "p99": q(0.99), "max": xs[len(xs)-1]}
}
