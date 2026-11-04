package main

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"statemig/internal/redisx"
)

// newSlowDeadBackend takes the connection and fails AUTH after delay: a dial that takes a while,
// then fails, the way a dial to a dead backend does.
func newSlowDeadBackend(t *testing.T, delay time.Duration) *redisx.Endpoint {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			nc, err := ln.Accept()
			if err != nil {
				return
			}
			t.Cleanup(func() { nc.Close() })
			go func() {
				redisx.ReadCommand(bufio.NewReader(nc))
				time.Sleep(delay)
				nc.Write([]byte("-ERR backend is gone\r\n"))
			}()
		}
	}()
	eps, _, err := redisx.ParseEndpoints(fmt.Sprintf("dead=redis://u:p@%s", ln.Addr()))
	if err != nil {
		t.Fatal(err)
	}
	return eps["dead"]
}

func switchTo(t *testing.T, p *Proxy, ep *redisx.Endpoint) {
	t.Helper()
	p.Pause(time.Millisecond) // nothing to wait for here: the gate is what SWITCH needs
	r, _ := p.Route()
	if err := p.Switch(Route{Gen: r.Gen + 1, Active: ep, Consume: ep}, 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	p.Resume()
}

// A command caught dialling a dead backend when the route moves was never sent: it goes to the new
// route instead of failing after the failure rollback is over.
func TestDialFailureAfterRouteMoveGoesToNewRoute(t *testing.T) {
	dead, good := newSlowDeadBackend(t, 300*time.Millisecond), newEchoRedis(t, "aws", 0)
	p, addr := startGatedProxy(t, dead)
	nc, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	send(t, nc, "SET", "session:a", "1")
	time.Sleep(100 * time.Millisecond)
	switchTo(t, p, good.ep)
	if got := readReply(t, nc, bufio.NewReader(nc)); !strings.Contains(got, "SET session:a 1") {
		t.Fatalf("got %q, want it answered by the new route", got)
	}
}

// With the route still in place the failure is real, and the client hears it.
func TestDialFailureWithoutRouteMoveFails(t *testing.T) {
	dead := newSlowDeadBackend(t, 10*time.Millisecond)
	_, addr := startGatedProxy(t, dead)
	nc, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	send(t, nc, "SET", "session:a", "1")
	if got := readReply(t, nc, bufio.NewReader(nc)); !strings.Contains(got, "upstream unavailable") {
		t.Fatalf("got %q, want an upstream error", got)
	}
}

// A transaction stays on the backend it started on: its commands are not moved to the new route.
func TestDialFailureInTransactionIsNotMoved(t *testing.T) {
	old, good := newEchoRedis(t, "old", 0), newEchoRedis(t, "aws", 0)
	p, addr := startGatedProxy(t, old.ep)
	nc, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	r := bufio.NewReader(nc)
	send(t, nc, "MULTI")
	readReply(t, nc, r)
	dead := newSlowDeadBackend(t, 10*time.Millisecond)
	s := anySession(t, p)
	p.mu.Lock()
	s.txEP = dead // the transaction's backend is lost: its upstream has to be dialled again
	delete(s.ups, "old")
	p.mu.Unlock()
	switchTo(t, p, good.ep)
	send(t, nc, "SET", "k", "1")
	if got := readReply(t, nc, r); !strings.Contains(got, "upstream unavailable") {
		t.Fatalf("got %q, want an upstream error", got)
	}
	if n := good.cmds.Load(); n != 0 {
		t.Fatalf("%d commands of the transaction reached the new route", n)
	}
}

func anySession(t *testing.T, p *Proxy) *session {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	for s := range p.sessions {
		return s
	}
	t.Fatal("no session")
	return nil
}
