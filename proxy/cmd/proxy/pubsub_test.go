package main

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"statemig/internal/redisx"
)

// fakeRedis answers SUBSCRIBE and PING like a subscribed connection, and records every command.
// With flood set it sends subscribers messages without end and never answers PING.
type fakeRedis struct {
	ep    *redisx.Endpoint
	flood bool
	mu    sync.Mutex
	seen  []string
}

func newFakeRedis(t *testing.T, name string, flood bool) *fakeRedis {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	f := &fakeRedis{ep: &redisx.Endpoint{Name: name, Addr: ln.Addr().String()}, flood: flood}
	go func() {
		for {
			nc, err := ln.Accept()
			if err != nil {
				return
			}
			t.Cleanup(func() { nc.Close() })
			go f.serve(nc)
		}
	}()
	return f
}

func (f *fakeRedis) serve(nc net.Conn) {
	r := bufio.NewReader(nc)
	for {
		args, err := redisx.ReadCommand(r)
		if err != nil {
			return
		}
		name := strings.ToLower(string(args[0]))
		f.mu.Lock()
		f.seen = append(f.seen, name)
		f.mu.Unlock()
		switch name {
		case "subscribe", "psubscribe":
			for i, a := range args[1:] {
				fmt.Fprintf(nc, "*3\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n:%d\r\n", len(name), name, len(a), a, i+1)
			}
			if f.flood {
				go flood(nc)
			}
		case "ping":
			if !f.flood {
				fmt.Fprintf(nc, "*2\r\n$4\r\npong\r\n$%d\r\n%s\r\n", len(args[1]), args[1])
			}
		default:
			fmt.Fprint(nc, "+OK\r\n")
		}
	}
}

func flood(nc net.Conn) {
	body := bytes.Repeat([]byte("x"), 1<<20)
	for {
		if _, err := fmt.Fprintf(nc, "*3\r\n$7\r\nmessage\r\n$1\r\na\r\n$%d\r\n%s\r\n", len(body), body); err != nil {
			return
		}
	}
}

func (f *fakeRedis) saw(cmd string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, c := range f.seen {
		if c == cmd {
			return true
		}
	}
	return false
}

func startProxy(t *testing.T, active *fakeRedis) (*Proxy, string) {
	t.Helper()
	p := &Proxy{cmds: CmdTable{"set": true, "del": true}, keys: KeyTable{"del": {1, -1, 1}},
		route: Route{Gen: 1, Active: active.ep, Consume: active.ep}, sessions: map[*session]struct{}{},
		pendingCache: map[string]int{}, latency: newLatencyWindow(16), blockCap: 200, upstreamTimeout: time.Second}
	p.cond = sync.NewCond(&p.mu)
	p.Stats.LastPauseMs.Store(0.0)
	p.Stats.MaxPauseMs.Store(0.0)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go p.Serve(ln)
	return p, ln.Addr().String()
}

// subscribe connects a client that holds a subscription through the proxy. It does not read past the
// confirmation until the test does.
func subscribe(t *testing.T, addr string, p *Proxy) (net.Conn, *bufio.Reader) {
	t.Helper()
	nc, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { nc.Close() })
	send(t, nc, "SUBSCRIBE", "a")
	r := bufio.NewReader(nc)
	if _, err := redisx.ReadRawReply(r, nil); err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(2 * time.Second); len(p.subSessions()) == 0; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("session never became a subscription")
		}
	}
	return nc, r
}

func send(t *testing.T, nc net.Conn, args ...string) {
	t.Helper()
	bs := make([][]byte, len(args))
	for i, a := range args {
		bs[i] = []byte(a)
	}
	w := bufio.NewWriter(nc)
	redisx.WriteCommand(w, bs...)
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
}

func readReply(t *testing.T, nc net.Conn, r *bufio.Reader) string {
	t.Helper()
	nc.SetReadDeadline(time.Now().Add(2 * time.Second))
	b, err := redisx.ReadRawReply(r, nil)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// A subscriber that stops reading must not hold a switch, and with it every client's writes.
func TestStuckSubscriberDoesNotHoldSwitch(t *testing.T) {
	aws, gcp := newFakeRedis(t, "aws", true), newFakeRedis(t, "gcp", false)
	p, addr := startProxy(t, aws)
	subscribe(t, addr, p) // never reads again
	time.Sleep(500 * time.Millisecond)

	if _, err := p.Pause(time.Second); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	t0 := time.Now()
	go func() { done <- p.Switch(Route{Gen: 2, Active: gcp.ep, Consume: gcp.ep}, time.Second) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("switch still waiting on a subscriber that does not read")
	}
	if d := time.Since(t0); d > 2*time.Second {
		t.Errorf("switch took %v", d)
	}
	if n := p.Stats.PubsubDropped.Load(); n != 1 {
		t.Errorf("pubsub_dropped = %d, want 1", n)
	}
}

// A switch keeps to its limit with a stuck client on the old side.
func TestSwitchKeepsToItsLimitWithStuckSubscriber(t *testing.T) {
	aws, gcp := newFakeRedis(t, "aws", true), newFakeRedis(t, "gcp", false)
	p, addr := startProxy(t, aws)
	subscribe(t, addr, p)
	time.Sleep(500 * time.Millisecond)

	if _, err := p.Pause(time.Second); err != nil {
		t.Fatal(err)
	}
	t0 := time.Now()
	if err := p.Switch(Route{Gen: 2, Active: gcp.ep, Consume: gcp.ep}, 200*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(t0); d > 500*time.Millisecond {
		t.Errorf("switch took %v with a 200ms limit", d)
	}
}

// A target that accepts connections and never answers AUTH costs a switch its limit, not a hang.
func TestSwitchKeepsToItsLimitWithSilentTarget(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	eps, _, err := redisx.ParseEndpoints("silent=redis://:pw@" + ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	silent := eps["silent"]

	aws := newFakeRedis(t, "aws", false)
	p, addr := startProxy(t, aws)
	subscribe(t, addr, p)
	if _, err := p.Pause(time.Second); err != nil {
		t.Fatal(err)
	}
	t0 := time.Now()
	if err := p.Switch(Route{Gen: 2, Active: silent, Consume: silent}, 200*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(t0); d > 500*time.Millisecond {
		t.Errorf("switch took %v with a 200ms limit", d)
	}
	if n := p.Stats.PubsubDropped.Load(); n != 1 {
		t.Errorf("pubsub_dropped = %d, want 1", n)
	}
}

// A healthy subscriber keeps its connection and hears from the new backend after a switch.
func TestSwitchMovesSubscriber(t *testing.T) {
	aws, gcp := newFakeRedis(t, "aws", false), newFakeRedis(t, "gcp", false)
	p, addr := startProxy(t, aws)
	nc, r := subscribe(t, addr, p)

	if _, err := p.Pause(time.Second); err != nil {
		t.Fatal(err)
	}
	if err := p.Switch(Route{Gen: 2, Active: gcp.ep, Consume: gcp.ep}, time.Second); err != nil {
		t.Fatal(err)
	}
	p.Resume()
	if n := p.Stats.PubsubMoved.Load(); n != 1 {
		t.Fatalf("pubsub_moved = %d, want 1", n)
	}
	send(t, nc, "PING", "after-move")
	if got := readReply(t, nc, r); !strings.Contains(got, "after-move") {
		t.Fatalf("PING reply %q", got)
	}
	if !gcp.saw("subscribe") {
		t.Error("the new backend has no subscription")
	}
}

// Only subscription commands get through a subscribed session; the rest never reach a backend.
func TestSubscribedSessionRefusesOtherCommands(t *testing.T) {
	aws := newFakeRedis(t, "aws", false)
	p, addr := startProxy(t, aws)
	nc, r := subscribe(t, addr, p)

	for _, cmd := range [][]string{{"FLUSHALL"}, {"CONFIG", "SET", "maxmemory", "1"}, {"XGROUP", "DESTROY", "q:a", "g"},
		{"SET", "cache:x", "1"}, {"GET", "cache:x"}, {"MULTI"}, {"RESET"}, {"SSUBSCRIBE", "s"}} {
		send(t, nc, cmd...)
		if got := readReply(t, nc, r); !strings.HasPrefix(got, "-ERR ") {
			t.Errorf("%v: reply %q", cmd, got)
		}
		if name := strings.ToLower(cmd[0]); aws.saw(name) {
			t.Errorf("%v reached the backend", cmd)
		}
	}
	send(t, nc, "SUBSCRIBE", "b")
	if got := readReply(t, nc, r); !strings.Contains(got, "subscribe") {
		t.Errorf("SUBSCRIBE reply %q", got)
	}
}

// A refused command during PAUSE leaves no held cache key behind, and does not wait at the gate.
func TestSubscribedSessionLeavesNoPendingCacheKey(t *testing.T) {
	aws := newFakeRedis(t, "aws", false)
	p, addr := startProxy(t, aws)
	nc, r := subscribe(t, addr, p)

	if _, err := p.Pause(time.Second); err != nil {
		t.Fatal(err)
	}
	send(t, nc, "DEL", "cache:x")
	if got := readReply(t, nc, r); !strings.HasPrefix(got, "-ERR ") {
		t.Fatalf("DEL reply %q", got)
	}
	p.Resume()
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.pendingCache) != 0 {
		t.Errorf("pendingCache = %v", p.pendingCache)
	}
}
