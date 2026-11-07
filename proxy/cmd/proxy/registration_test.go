package main

import (
	"bufio"
	"encoding/json"
	"net"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"statemig/internal/redisx"
)

func TestUnboundProxyRegistersCredentialsWithoutExposingThem(t *testing.T) {
	s := &server{endpoints: map[string]*redisx.Endpoint{}}
	w := httptest.NewRecorder()
	s.status(w, httptest.NewRequest("GET", "/status", nil))
	if !strings.Contains(w.Body.String(), `"bound":false`) {
		t.Fatal(w.Body.String())
	}
	w = httptest.NewRecorder()
	s.registerBackend(w, httptest.NewRequest("POST", "/backends", strings.NewReader(`{"name":"source","url":"redis://app:private-password@localhost:6379"}`)))
	if w.Code != 201 || s.endpoints["source"] == nil {
		t.Fatal(w.Code, w.Body.String())
	}
	if strings.Contains(w.Body.String(), "private-password") || strings.Contains(w.Body.String(), "redis://") {
		t.Fatal("credentials exposed")
	}
	if s.proxy != nil {
		t.Fatal("registering a backend initialized a route")
	}
}

func TestBoundBackendAndClientAccountsCannotBeReplaced(t *testing.T) {
	ep, _ := redisx.ParseEndpoint("source", "redis://app:original@localhost:6379")
	p := &Proxy{route: Route{Gen: 8, Active: ep, Consume: ep}}
	s := &server{proxy: p, endpoints: map[string]*redisx.Endpoint{"source": ep}, clients: map[string]Client{"app": {Password: "fixed", Role: RoleApp}}}
	w := httptest.NewRecorder()
	s.registerBackend(w, httptest.NewRequest("POST", "/backends", strings.NewReader(`{"name":"source","url":"redis://app:replacement@localhost:6379"}`)))
	if w.Code != 409 {
		t.Fatal(w.Code, w.Body.String())
	}
	route, _ := p.Route()
	if route.Gen != 8 || s.endpoints["source"] != ep || s.clients["app"].Password != "fixed" {
		t.Fatal("active binding changed")
	}
}

func TestBindRequiresRecordAndDoesNotChooseAHistoricalBackend(t *testing.T) {
	active := newEchoRedis(t, "active", 0)
	s := &server{endpoints: map[string]*redisx.Endpoint{"active": active.ep, "retired": {Name: "retired", Addr: "127.0.0.1:1"}}, controlFile: filepath.Join(t.TempDir(), "control.json"), clients: map[string]Client{"app": {Password: "fixed", Role: RoleApp}}}
	w := httptest.NewRecorder()
	s.bind(w, httptest.NewRequest("POST", "/bind", nil))
	if w.Code != 409 || s.proxy != nil {
		t.Fatal("missing record was accepted")
	}
	body, _ := json.Marshal(controlRecord{Gen: 7, Active: "active", Consume: "active"})
	if err := os.WriteFile(s.controlFile, body, 0600); err != nil {
		t.Fatal(err)
	}
	w = httptest.NewRecorder()
	s.bind(w, httptest.NewRequest("POST", "/bind", nil))
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	route, _ := s.proxy.Route()
	if route.Active != active.ep || route.Gen != 7 || s.proxy.clients["app"].Password != "fixed" {
		t.Fatal("record or account was not preserved")
	}
}

func TestRESPAcceptDoesNotWaitForTheAdminUpdateLock(t *testing.T) {
	backend := newEchoRedis(t, "active", 0)
	p, _ := startGatedProxy(t, backend.ep)
	s := &server{proxy: p}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go s.serve(ln)
	s.mu.Lock()
	defer s.mu.Unlock()
	nc, err := net.DialTimeout("tcp", ln.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	nc.SetDeadline(time.Now().Add(time.Second))
	w := bufio.NewWriter(nc)
	redisx.WriteCommand(w, [][]byte{[]byte("GET"), []byte("session:test")}...)
	if err = w.Flush(); err != nil {
		t.Fatal(err)
	}
	if _, err = redisx.ReadValue(bufio.NewReader(nc)); err != nil {
		t.Fatal("RESP blocked by admin update", err)
	}
}

func TestRegistrationRejectsCommandsMissingFromTheNewBackend(t *testing.T) {
	backend := newEchoRedis(t, "target", 0) // its COMMAND result contains no command entries
	active := &redisx.Endpoint{Name: "source", Addr: "127.0.0.1:1"}
	p := &Proxy{route: Route{Active: active, Consume: active}, cmds: CmdTable{"set": true}, keys: KeyTable{}}
	s := &server{proxy: p, endpoints: map[string]*redisx.Endpoint{"source": active}}
	body, _ := json.Marshal(map[string]string{"name": "target", "url": "redis://" + backend.ep.Addr})
	w := httptest.NewRecorder()
	s.registerBackend(w, httptest.NewRequest("POST", "/backends", strings.NewReader(string(body))))
	if w.Code != 409 || s.endpoints["target"] != nil {
		t.Fatal("backend missing an existing command was accepted", w.Code)
	}
}
