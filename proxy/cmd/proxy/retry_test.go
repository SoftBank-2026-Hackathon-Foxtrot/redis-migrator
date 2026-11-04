package main

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"sync/atomic"
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

// newDroppingBackend answers +OK to the first command on a connection and then hangs up, and
// counts every command it was ever sent. It serves new connections the same way.
func newDroppingBackend(t *testing.T) (*redisx.Endpoint, *atomic.Int64) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	var cmds atomic.Int64
	go func() {
		for {
			nc, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer nc.Close()
				if _, err := redisx.ReadCommand(bufio.NewReader(nc)); err == nil {
					cmds.Add(1)
					nc.Write([]byte("+OK\r\n"))
				}
			}()
		}
	}()
	return &redisx.Endpoint{Name: "drop", Addr: ln.Addr().String()}, &cmds
}

// Redis drops a transaction with its connection. The client must hear that as a closed connection:
// otherwise the commands it sends next run outside any transaction on a new connection.
func TestLostTransactionClosesClient(t *testing.T) {
	be, cmds := newDroppingBackend(t)
	p, addr := startGatedProxy(t, be)
	nc, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	r := bufio.NewReader(nc)
	send(t, nc, "MULTI")
	readReply(t, nc, r)
	send(t, nc, "SET", "a", "1")
	if got := readReply(t, nc, r); !strings.Contains(got, "upstream unavailable") {
		t.Fatalf("got %q, want an upstream error", got)
	}
	nc.SetReadDeadline(time.Now().Add(2 * time.Second))
	send(t, nc, "SET", "b", "2")
	if line, err := r.ReadString('\n'); err == nil {
		t.Fatalf("connection still open after the transaction was lost, got %q", line)
	} else if ne, ok := err.(net.Error); ok && ne.Timeout() {
		t.Fatal("connection still open after the transaction was lost: no reply, no close")
	}
	if n := cmds.Load(); n > 2 {
		t.Fatalf("backend saw %d commands, the ones after the lost transaction reached it", n)
	}
	for deadline := time.Now().Add(2 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		p.mu.Lock()
		open := p.tx
		p.mu.Unlock()
		if open == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d transactions still counted open: PAUSE would wait on them", open)
		}
	}
}

// MULTI itself failing is the same: the commands pipelined behind it must not run on their own.
func TestFailedMultiClosesClient(t *testing.T) {
	dead := newSlowDeadBackend(t, 10*time.Millisecond)
	_, addr := startGatedProxy(t, dead)
	nc, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	r := bufio.NewReader(nc)
	send(t, nc, "MULTI")
	if got := readReply(t, nc, r); !strings.Contains(got, "upstream unavailable") {
		t.Fatalf("got %q, want an upstream error", got)
	}
	nc.SetReadDeadline(time.Now().Add(2 * time.Second))
	if line, err := r.ReadString('\n'); err == nil {
		t.Fatalf("connection still open after MULTI failed, got %q", line)
	} else if ne, ok := err.(net.Error); ok && ne.Timeout() {
		t.Fatal("connection still open after MULTI failed")
	}
}

// newSilentBackend takes connections and never answers.
func newSilentBackend(t *testing.T) string {
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
		}
	}()
	return ln.Addr().String()
}

// A backend that takes the connection and never answers AUTH must fail the dial, not hang it.
func TestDialToSilentBackendFails(t *testing.T) {
	eps, _, err := redisx.ParseEndpoints(fmt.Sprintf("silent=redis://u:p@%s", newSilentBackend(t)))
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		c, err := eps["silent"].Dial(0)
		if c != nil {
			c.Close()
		}
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("dial succeeded against a backend that never answered")
		}
	case <-time.After(8 * time.Second):
		t.Fatal("dial still hanging after 8s")
	}
}

// Dial leaves no deadline behind: its callers hold connections open for long reads.
func TestDialClearsItsDeadline(t *testing.T) {
	be := newEchoRedis(t, "aws", 0)
	c, err := be.ep.Dial(0)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	time.Sleep(5200 * time.Millisecond) // past the dial's own bound
	if _, err := c.DoStr("PING"); err != nil {
		t.Fatalf("connection unusable after the dial's deadline passed: %v", err)
	}
}

// The HELLO replayed on a new upstream has a deadline too: a silent backend fails the command.
func TestHelloReplayToSilentBackendFails(t *testing.T) {
	first := newEchoRedis(t, "aws", 0)
	p, addr := startGatedProxy(t, first.ep)
	p.upstreamTimeout = 200 * time.Millisecond
	nc, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	r := bufio.NewReader(nc)
	send(t, nc, "HELLO", "2")
	readReply(t, nc, r)
	switchTo(t, p, &redisx.Endpoint{Name: "silent", Addr: newSilentBackend(t)})
	nc.SetReadDeadline(time.Now().Add(3 * time.Second))
	send(t, nc, "GET", "k")
	if got := readReply(t, nc, r); !strings.Contains(got, "upstream unavailable") {
		t.Fatalf("got %q, want an upstream error", got)
	}
}
