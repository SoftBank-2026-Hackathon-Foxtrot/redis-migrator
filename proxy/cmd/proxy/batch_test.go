package main

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"statemig/internal/redisx"
)

// echoRedis answers every command with its arguments joined by spaces, after delay, and counts
// the reads it needed: a pipeline forwarded as one write is read in a few.
type echoRedis struct {
	ep    *redisx.Endpoint
	delay time.Duration
	reads atomic.Int64
	cmds  atomic.Int64
}

func newEchoRedis(t *testing.T, name string, delay time.Duration) *echoRedis {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	e := &echoRedis{ep: &redisx.Endpoint{Name: name, Addr: ln.Addr().String()}, delay: delay}
	go func() {
		for {
			nc, err := ln.Accept()
			if err != nil {
				return
			}
			t.Cleanup(func() { nc.Close() })
			go e.serve(nc)
		}
	}()
	return e
}

func (e *echoRedis) serve(nc net.Conn) {
	r, w := bufio.NewReader(nc), bufio.NewWriter(nc)
	for {
		args, err := redisx.ReadCommand(r)
		if err != nil {
			return
		}
		e.cmds.Add(1)
		parts := make([]string, len(args))
		for i, a := range args {
			parts[i] = string(a)
		}
		body := strings.Join(parts, " ")
		fmt.Fprintf(w, "$%d\r\n%s\r\n", len(body), body)
		if r.Buffered() == 0 {
			e.reads.Add(1)
			time.Sleep(e.delay)
			w.Flush()
		}
	}
}

func startGatedProxy(t *testing.T, active *redisx.Endpoint) (*Proxy, string) {
	t.Helper()
	p, addr := startProxy(t, &fakeRedis{ep: active})
	p.mu.Lock()
	p.publishGate()
	p.mu.Unlock()
	return p, addr
}

func pipeline(t *testing.T, nc net.Conn, cmds [][]string) {
	t.Helper()
	w := bufio.NewWriter(nc)
	for _, c := range cmds {
		bs := make([][]byte, len(c))
		for i, a := range c {
			bs[i] = []byte(a)
		}
		redisx.WriteCommand(w, bs...)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
}

// A pipeline goes upstream in a few writes, and every reply comes back in order, also around a
// command that goes alone (SELECT) and one the proxy refuses itself.
func TestPipelineForwardedInBatchesInOrder(t *testing.T) {
	be := newEchoRedis(t, "aws", 0)
	_, addr := startGatedProxy(t, be.ep)
	nc, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	var cmds [][]string
	var want []string
	for i := 0; i < 1000; i++ {
		switch i {
		case 300:
			cmds = append(cmds, []string{"SELECT", "0"})
			want = append(want, "SELECT 0")
		case 600:
			cmds = append(cmds, []string{"FLUSHALL"})
			want = append(want, "-ERR flushall is not allowed through the proxy")
		default:
			k := fmt.Sprintf("GET k%d", i)
			cmds = append(cmds, strings.Fields(k))
			want = append(want, k)
		}
	}
	pipeline(t, nc, cmds)
	r := bufio.NewReader(nc)
	for i, w := range want {
		got := readReply(t, nc, r)
		if !strings.Contains(got, w) {
			t.Fatalf("reply %d: got %q, want %q", i, got, w)
		}
	}
	if n := be.reads.Load(); n > 100 {
		t.Fatalf("backend needed %d reads for %d commands: not batched", n, be.cmds.Load())
	}
}

// A PAUSE that comes while a session has writes queued must not wait on them forever: the
// session answers what it queued before its next write waits at the gate.
func TestPauseDuringPipelinedWrites(t *testing.T) {
	be := newEchoRedis(t, "aws", 20*time.Millisecond)
	p, addr := startGatedProxy(t, be.ep)
	nc, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	r := bufio.NewReader(nc)
	var mu sync.Mutex
	var replies []string
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 400; i++ {
			nc.SetReadDeadline(time.Now().Add(5 * time.Second))
			b, err := redisx.ReadRawReply(r, nil)
			if err != nil {
				return
			}
			mu.Lock()
			replies = append(replies, string(b))
			mu.Unlock()
		}
	}()
	var cmds [][]string
	for i := 0; i < 200; i++ {
		cmds = append(cmds, []string{"SET", fmt.Sprintf("k%d", i), "v"})
	}
	pipeline(t, nc, cmds)
	time.Sleep(5 * time.Millisecond) // part of it queued upstream
	if _, err := p.Pause(time.Second); err != nil {
		t.Fatalf("pause: %v", err)
	}
	pipeline(t, nc, cmds) // held at the gate
	time.Sleep(50 * time.Millisecond)
	mu.Lock()
	before := len(replies)
	mu.Unlock()
	if before > 200 {
		t.Fatalf("%d replies while paused: writes passed the gate", before)
	}
	p.Resume()
	<-done
	if len(replies) != 400 {
		t.Fatalf("got %d replies, want 400", len(replies))
	}
	for i, b := range replies {
		if want := fmt.Sprintf("SET k%d v", i%200); !bytes.Contains([]byte(b), []byte(want)) {
			t.Fatalf("reply %d: %q, want %q", i, b, want)
		}
	}
}
