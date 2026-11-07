package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"net/http/httptest"
	"strings"
	"testing"

	"statemig/internal/redisx"
)

// commandRedis answers COMMAND with the given entries (name -> write flag, keys 1,1,1), like a
// backend of another engine or version would.
func commandRedis(t *testing.T, name string, commands map[string]bool) *redisx.Endpoint {
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
			go func(nc net.Conn) {
				defer nc.Close()
				r, w := bufio.NewReader(nc), bufio.NewWriter(nc)
				for {
					args, err := redisx.ReadCommand(r)
					if err != nil {
						return
					}
					if !strings.EqualFold(string(args[0]), "COMMAND") {
						w.WriteString("+OK\r\n")
						w.Flush()
						continue
					}
					fmt.Fprintf(w, "*%d\r\n", len(commands))
					for command, write := range commands {
						flag := "readonly"
						if write {
							flag = "write"
						}
						fmt.Fprintf(w, "*6\r\n$%d\r\n%s\r\n:-2\r\n*1\r\n+%s\r\n:1\r\n:1\r\n:1\r\n", len(command), command, flag)
					}
					w.Flush()
				}
			}(nc)
		}
	}()
	return &redisx.Endpoint{Name: name, Addr: ln.Addr().String()}
}

func register(s *server, ep *redisx.Endpoint) *httptest.ResponseRecorder {
	body, _ := json.Marshal(map[string]string{"name": ep.Name, "url": "redis://" + ep.Addr})
	w := httptest.NewRecorder()
	s.registerBackend(w, httptest.NewRequest("POST", "/backends", strings.NewReader(string(body))))
	return w
}

// Memorystore 7.0, ElastiCache 7.1 and Valkey 8 differ in commands the application never sends.
// With a contract only the contract is compared; without one any difference refuses the backend.
func TestRegistrationComparesOnlyTheContractAcrossEngines(t *testing.T) {
	active := &redisx.Endpoint{Name: "source", Addr: "127.0.0.1:1"}
	contract := set("get", "set")
	newer := commandRedis(t, "target", map[string]bool{"get": false, "set": true, "waitaof": false})

	s := &server{proxy: &Proxy{route: Route{Active: active, Consume: active}, allowedCommands: contract,
		cmds: CmdTable{"get": false, "set": true}, keys: KeyTable{"get": {1, 1, 1}, "set": {1, 1, 1}}},
		endpoints: map[string]*redisx.Endpoint{"source": active}}
	if w := register(s, newer); w.Code != 201 || s.endpoints["target"] == nil {
		t.Fatal("a backend with extra commands outside the contract was refused", w.Code, w.Body.String())
	}

	s = &server{proxy: &Proxy{route: Route{Active: active, Consume: active},
		cmds: CmdTable{"get": false, "set": true}, keys: KeyTable{"get": {1, 1, 1}, "set": {1, 1, 1}}},
		endpoints: map[string]*redisx.Endpoint{"source": active}}
	if w := register(s, newer); w.Code != 409 {
		t.Fatal("without a contract a different command table was accepted", w.Code)
	}

	missing := commandRedis(t, "target", map[string]bool{"get": false, "waitaof": false})
	s = &server{proxy: &Proxy{route: Route{Active: active, Consume: active}, allowedCommands: contract,
		cmds: CmdTable{"get": false, "set": true}, keys: KeyTable{"get": {1, 1, 1}, "set": {1, 1, 1}}},
		endpoints: map[string]*redisx.Endpoint{"source": active}}
	if w := register(s, missing); w.Code != 409 || !strings.Contains(w.Body.String(), "set") {
		t.Fatal("a backend missing a contract command was accepted", w.Code, w.Body.String())
	}
}

// AUTH is answered by the proxy and never reaches a backend, so a contract need not list it.
func TestAuthWorksWithoutBeingInTheContract(t *testing.T) {
	backend := newEchoRedis(t, "source", 0)
	p, addr := startGatedProxy(t, backend.ep)
	p.clients = map[string]Client{"app": {Password: "pw", Role: RoleApp}}
	p.allowedCommands = set("get")
	c, err := redisx.ParseEndpoint("client", "redis://app:pw@"+addr)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := c.Dial(0)
	if err != nil {
		t.Fatal("AUTH refused by the contract", err)
	}
	defer conn.Close()
	if v, err := conn.DoStr("GET", "session:a"); err != nil || v.IsErr() {
		t.Fatal("contract command refused", v, err)
	}
	if v, err := conn.DoStr("SET", "session:a", "1"); err == nil && !v.IsErr() {
		t.Fatal("command outside the contract reached the backend")
	}
}
