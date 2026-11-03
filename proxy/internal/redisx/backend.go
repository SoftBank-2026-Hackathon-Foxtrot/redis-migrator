package redisx

import (
	"bufio"
	"crypto/tls"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Endpoint is one Redis deployment (ElastiCache, Memorystore, on-prem...).
type Endpoint struct {
	Name     string `json:"name"`
	Addr     string `json:"addr"`
	TLS      bool   `json:"tls"`
	username string
	password string
}

// ParseEndpoints parses "aws=redis://host:6379,gcp=rediss://:pw@host:6378".
func ParseEndpoints(spec string) (map[string]*Endpoint, []string, error) {
	eps := map[string]*Endpoint{}
	var order []string
	for _, item := range strings.Split(spec, ",") {
		name, raw, ok := strings.Cut(strings.TrimSpace(item), "=")
		if !ok {
			return nil, nil, fmt.Errorf("bad endpoint %q (want name=url)", item)
		}
		u, err := url.Parse(raw)
		if err != nil {
			return nil, nil, err
		}
		if u.Scheme != "redis" && u.Scheme != "rediss" {
			return nil, nil, fmt.Errorf("unsupported scheme %q", u.Scheme)
		}
		ep := &Endpoint{Name: name, Addr: u.Host, TLS: u.Scheme == "rediss"}
		if u.Port() == "" {
			ep.Addr = net.JoinHostPort(u.Hostname(), "6379")
		}
		if u.User != nil {
			ep.username = u.User.Username()
			ep.password, _ = u.User.Password()
		}
		eps[name] = ep
		order = append(order, name)
	}
	return eps, order, nil
}

// Conn is a plain synchronous RESP connection.
type Conn struct {
	nc net.Conn
	r  *bufio.Reader
	w  *bufio.Writer
	db int // the db this connection is on, as far as Select knows
}

func (e *Endpoint) Dial(db int) (*Conn, error) {
	d := net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}
	var nc net.Conn
	var err error
	if e.TLS {
		nc, err = tls.DialWithDialer(&d, "tcp", e.Addr, &tls.Config{InsecureSkipVerify: true}) //nolint:gosec // managed Redis certs are not in the image trust store
	} else {
		nc, err = d.Dial("tcp", e.Addr)
	}
	if err != nil {
		return nil, err
	}
	c := &Conn{nc: nc, r: bufio.NewReaderSize(nc, 64<<10), w: bufio.NewWriterSize(nc, 64<<10)}
	if e.password != "" {
		args := []string{"AUTH"}
		if e.username != "" {
			args = append(args, e.username)
		}
		if _, err := c.DoStr(append(args, e.password)...); err != nil {
			c.Close()
			return nil, fmt.Errorf("auth %s: %w", e.Name, err)
		}
	}
	if err := c.Select(db); err != nil {
		c.Close()
		return nil, err
	}
	return c, nil
}

// Select moves the connection to db, sending SELECT only when it is on another one.
func (c *Conn) Select(db int) error {
	if c.db == db {
		return nil
	}
	if _, err := c.DoStr("SELECT", strconv.Itoa(db)); err != nil {
		return err
	}
	c.db = db
	return nil
}

func (c *Conn) Close() error { return c.nc.Close() }

// Do sends one command and returns the parsed reply; a Redis error reply is returned as err.
func (c *Conn) Do(args ...[]byte) (Value, error) {
	WriteCommand(c.w, args...)
	if err := c.w.Flush(); err != nil {
		return Value{}, err
	}
	v, err := ReadValue(c.r)
	if err != nil {
		return v, err
	}
	return v, v.Err()
}

func (c *Conn) DoStr(args ...string) (Value, error) {
	b := make([][]byte, len(args))
	for i, a := range args {
		b[i] = []byte(a)
	}
	return c.Do(b...)
}

// Pipeline sends cmds in one round trip. A Redis error reply stays in its Value.
func (c *Conn) Pipeline(cmds ...[][]byte) ([]Value, error) {
	for _, cmd := range cmds {
		WriteCommand(c.w, cmd...)
	}
	if err := c.w.Flush(); err != nil {
		return nil, err
	}
	out := make([]Value, len(cmds))
	for i := range cmds {
		v, err := ReadValue(c.r)
		if err != nil {
			return nil, err
		}
		out[i] = v
	}
	return out, nil
}

// Tx runs cmds inside MULTI/EXEC and returns the EXEC array.
func (c *Conn) Tx(cmds ...[][]byte) ([]Value, error) {
	WriteCommand(c.w, []byte("MULTI"))
	for _, cmd := range cmds {
		WriteCommand(c.w, cmd...)
	}
	WriteCommand(c.w, []byte("EXEC"))
	if err := c.w.Flush(); err != nil {
		return nil, err
	}
	var firstErr error
	for i := 0; i < len(cmds)+1; i++ { // +OK for MULTI, +QUEUED per command
		v, err := ReadValue(c.r)
		if err != nil {
			return nil, err
		}
		if v.IsErr() && firstErr == nil {
			firstErr = v.Err()
		}
	}
	v, err := ReadValue(c.r)
	if err != nil {
		return nil, err
	}
	if firstErr != nil {
		return nil, firstErr
	}
	if v.IsErr() {
		return nil, v.Err()
	}
	for _, item := range v.Arr {
		if item.IsErr() {
			return v.Arr, item.Err()
		}
	}
	return v.Arr, nil
}

// B converts a string argument to bytes.
func B(s string) []byte { return []byte(s) }

// Writer, Reader and NetConn expose the buffered streams for raw relaying.
func (c *Conn) Writer() *bufio.Writer { return c.w }
func (c *Conn) Reader() *bufio.Reader { return c.r }
func (c *Conn) NetConn() net.Conn     { return c.nc }
