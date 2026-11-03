package main

// Which commands write? Uses the backend's own COMMAND table ("write" flag) so
// new commands and Valkey/Redis differences are handled without a hardcoded list.

import (
	"bytes"
	"strings"

	"statemig/internal/redisx"
)

type CmdTable map[string]bool // command name → has the "write" flag

// keySpec is where a command's keys are, from COMMAND (first, last, step).
type keySpec struct{ first, last, step int }

type KeyTable map[string]keySpec

func (k KeyTable) keysOf(name string, args [][]byte) [][]byte {
	sp, ok := k[name]
	if !ok || sp.first <= 0 || sp.step <= 0 { // no keys, or movable keys
		return nil
	}
	last := sp.last
	if last < 0 {
		last = len(args) + last
	}
	var out [][]byte
	for i := sp.first; i <= last && i < len(args); i += sp.step {
		out = append(out, args[i])
	}
	return out
}

// Role is what an authenticated client may send through the proxy.
type Role string

const (
	RoleApp    Role = "app"
	RoleWorker Role = "worker"
)

var (
	queueAdminCmds = set("xdel", "xtrim", "xsetid", "del", "unlink")
	appDeniedCmds  = set("xgroup", "eval", "evalsha", "fcall", "script", "function")
)

const cachePrefix = "cache:"

func isCacheKey(key []byte) bool { return bytes.HasPrefix(key, []byte(cachePrefix)) }

func cacheMiss(resp3 bool) []byte {
	if resp3 {
		return []byte("_\r\n")
	}
	return []byte("$-1\r\n")
}

// cacheReply answers a cache command around a PAUSE. A read goes to Redis unless a write
// to that key is being held: it would return what that write is about to change, so it
// is a miss and cache-aside reads the DB. A fill is dropped, since one held until RESUME
// could land after the DEL of a commit made in the pause. A key stays pending until its
// held write has run, which can be just after RESUME. nil = not answered here.
func cacheReply(name string, args [][]byte, resp3, paused bool, pending map[string]int) []byte {
	if len(args) < 2 || !isCacheKey(args[1]) {
		return nil
	}
	held := pending[string(args[1])] > 0
	switch name {
	case "get":
		if held {
			return cacheMiss(resp3)
		}
	case "setex", "psetex":
		if paused || held {
			return []byte("+OK\r\n")
		}
	case "set":
		for _, a := range args[min(3, len(args)):] {
			if strings.EqualFold(string(a), "get") {
				return nil
			}
		}
		if paused || held {
			return []byte("+OK\r\n")
		}
	}
	return nil
}

// deny returns why the role may not send this command, or "" if it may.
func (r Role) deny(name string, args [][]byte, keys KeyTable) string {
	if r == "" {
		return ""
	}
	ks := keys.keysOf(name, args)
	switch r {
	case RoleApp:
		if consumeCmds[name] || appDeniedCmds[name] || name == "xdel" || name == "xtrim" {
			return "the app may not run " + name
		}
		for _, k := range ks {
			key := string(k)
			switch {
			case strings.HasPrefix(key, "q:"):
				if name != "xadd" && !(name == "zadd" && key == "q:delayed") {
					return "the app may only XADD to queues and schedule on q:delayed"
				}
			case !strings.HasPrefix(key, "session:") && !strings.HasPrefix(key, "cache:") && !strings.HasPrefix(key, "rl:"):
				return "the app may not touch key " + key
			}
		}
	case RoleWorker:
		if queueAdminCmds[name] {
			return "a worker may not run " + name
		}
		if name == "xgroup" && len(args) > 1 {
			if sub := strings.ToLower(string(args[1])); sub != "create" && sub != "createconsumer" {
				return "a worker may not run xgroup " + sub
			}
		}
		for _, k := range ks {
			if !strings.HasPrefix(string(k), "q:") {
				return "a worker may only touch q:* keys"
			}
		}
	}
	return ""
}

var (
	// Scripts may write anything; the COMMAND table doesn't flag them as writes.
	scriptCmds = set("eval", "evalsha", "fcall")
	// Starting a transaction is held during PAUSE like a write, so no new
	// transaction opens while the proxy waits for the open ones to finish.
	txStartCmds = set("multi", "watch")
	// Queue consumer commands: routed to the consume backend until DRAIN_DONE.
	consumeCmds = set("xreadgroup", "xack", "xautoclaim", "xclaim", "xpending", "xinfo")
	// Always rejected: whole-db / admin commands the app has no business sending.
	deniedCmds = set("flushall", "flushdb", "keys", "config", "swapdb", "move")
	pubsubCmds = set("subscribe", "psubscribe", "ssubscribe", "monitor")
)

func set(xs ...string) map[string]bool {
	m := map[string]bool{}
	for _, x := range xs {
		m[x] = true
	}
	return m
}

func LoadCmdTable(c *redisx.Conn) (CmdTable, KeyTable, error) {
	v, err := c.DoStr("COMMAND")
	if err != nil {
		return nil, nil, err
	}
	t, kt := CmdTable{}, KeyTable{}
	for _, e := range v.Arr {
		if len(e.Arr) < 3 {
			continue
		}
		write := false
		for _, f := range e.Arr[2].Arr {
			if string(f.Str) == "write" {
				write = true
			}
		}
		cmd := string(bytes.ToLower(e.Arr[0].Str))
		t[cmd] = write
		if len(e.Arr) >= 6 {
			kt[cmd] = keySpec{int(e.Arr[3].Int), int(e.Arr[4].Int), int(e.Arr[5].Int)}
		}
	}
	return t, kt, nil
}

// Held reports whether the command is held at the gate during PAUSE.
func (t CmdTable) Held(name string) bool {
	return t[name] || scriptCmds[name] || txStartCmds[name]
}
