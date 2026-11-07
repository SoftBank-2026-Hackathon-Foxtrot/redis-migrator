package main

import (
	"net/http/httptest"
	"strings"
	"testing"

	"statemig/internal/redisx"
)

func TestRegisterBackendPreservesRunningSyncAndCredentials(t *testing.T) {
	ep, _ := redisx.ParseEndpoint("source", "redis://admin:original@localhost:6379")
	sy := &Syncer{}
	s := &server{endpoints: map[string]*redisx.Endpoint{"source": ep}, sy: sy}
	w := httptest.NewRecorder()
	s.registerBackend(w, httptest.NewRequest("POST", "/backends", strings.NewReader(`{"name":"source","url":"redis://admin:replacement@localhost:6379"}`)))
	if w.Code != 409 || s.sy != sy || s.endpoints["source"] != ep {
		t.Fatal("running sync was replaced")
	}
	w = httptest.NewRecorder()
	s.registerBackend(w, httptest.NewRequest("POST", "/backends", strings.NewReader(`{"name":"target","url":"redis://admin:private-password@localhost:6380"}`)))
	if w.Code != 201 || s.sy != sy {
		t.Fatal(w.Code, w.Body.String())
	}
	if strings.Contains(w.Body.String(), "private-password") || strings.Contains(w.Body.String(), "redis://") {
		t.Fatal("credential exposed")
	}
}
