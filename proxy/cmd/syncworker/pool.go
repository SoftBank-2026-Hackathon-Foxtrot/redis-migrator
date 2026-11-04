package main

// Connections kept for the steps inside a write pause. A new connection costs a TCP handshake
// and AUTH (two round trips, four with TLS) and, on a lossy path, a SYN retry of a second or
// more. Warmed just before the pause, the reconcile and the verify only send commands.

import (
	"errors"
	"io"
	"net"
	"sync"
	"time"

	"statemig/internal/redisx"
)

const (
	poolIdleMax = 20 * time.Second // older idle connections are dropped, not trusted
	poolSize    = verifyWorkers + 2
)

type idleConn struct {
	c     *redisx.Conn
	since time.Time
}

type connPool struct {
	mu   sync.Mutex
	idle map[string][]idleConn
}

var pool = &connPool{idle: map[string][]idleConn{}}

func poolKey(ep *redisx.Endpoint) string { return ep.Name + "|" + ep.Addr }

// get returns an idle connection if there is a fresh one, else a new one; reused says which.
func (p *connPool) get(ep *redisx.Endpoint) (c *redisx.Conn, reused bool, err error) {
	k := poolKey(ep)
	p.mu.Lock()
	for len(p.idle[k]) > 0 {
		n := len(p.idle[k])
		ic := p.idle[k][n-1]
		p.idle[k] = p.idle[k][:n-1]
		if time.Since(ic.since) < poolIdleMax {
			p.mu.Unlock()
			return ic.c, true, nil
		}
		ic.c.Close()
	}
	p.mu.Unlock()
	c, err = ep.Dial(0)
	return c, false, err
}

// put keeps a connection that worked; the caller closes one that failed instead.
func (p *connPool) put(ep *redisx.Endpoint, c *redisx.Conn) {
	k := poolKey(ep)
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.idle[k]) >= poolSize {
		c.Close()
		return
	}
	p.idle[k] = append(p.idle[k], idleConn{c, time.Now()})
}

// warm fills the pool for ep with fresh connections, dialled at once.
func (p *connPool) warm(ep *redisx.Endpoint) error {
	k := poolKey(ep)
	p.mu.Lock()
	var keep []idleConn
	for _, ic := range p.idle[k] {
		if time.Since(ic.since) < poolIdleMax/2 {
			keep = append(keep, ic)
		} else {
			ic.c.Close()
		}
	}
	p.idle[k] = keep
	need := poolSize - len(keep)
	p.mu.Unlock()
	var wg sync.WaitGroup
	errs := make(chan error, need)
	for i := 0; i < need; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, err := ep.Dial(0)
			if err != nil {
				errs <- err
				return
			}
			if _, err := c.DoStr("PING"); err != nil {
				c.Close()
				errs <- err
				return
			}
			p.put(ep, c)
		}()
	}
	wg.Wait()
	close(errs)
	return <-errs
}

// withConn runs fn on a pooled connection. A reused one that fails on the network is replaced
// once: it may have died while idle.
func (p *connPool) withConn(ep *redisx.Endpoint, fn func(*redisx.Conn) error) error {
	c, reused, err := p.get(ep)
	if err != nil {
		return err
	}
	err = fn(c)
	if err != nil && reused && isNetErr(err) {
		c.Close()
		if c, err = ep.Dial(0); err != nil {
			return err
		}
		err = fn(c)
	}
	if err != nil {
		c.Close()
		return err
	}
	p.put(ep, c)
	return nil
}

func isNetErr(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, net.ErrClosed)
}
