package main

// statemig-proxy: central RESP proxy for Redis migration. The control plane
// drives it through the admin HTTP API.
//
// env:
//   STATEMIG_ENDPOINTS      aws=redis://app:pw@redis-aws:6379,gcp=... (app ACL user)
//   STATEMIG_ACTIVE         backend to start on if no control record exists
//   STATEMIG_CONTROL_FILE   control record written by the control plane (stand-in
//                           for the management DB): {"gen","active","consume","paused"}
//   STATEMIG_LISTEN         RESP listen addr (default :6379)
//   STATEMIG_ADMIN          admin HTTP addr (default :7000)
//   STATEMIG_CLIENTS        accounts clients AUTH with: app:pw:app,worker:pw:worker
//                           (user:password:role); empty means no auth and no role limits
//   STATEMIG_ADMIN_TOKEN    bearer token required on the admin API
//
// The control plane writes the record first and then calls the admin API; the
// proxy only accepts a switch to the generation that is in the record. On
// restart the proxy starts from the record, paused if a cutover was in progress.

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"statemig/internal/redisx"
)

type controlRecord struct {
	Gen     uint64 `json:"gen"`
	Active  string `json:"active"`
	Consume string `json:"consume"`
	Paused  bool   `json:"paused"`
}

type server struct {
	mu          sync.Mutex
	pointerMu   sync.RWMutex
	proxy       *Proxy
	clients     map[string]Client
	endpoints   map[string]*redisx.Endpoint
	order       []string
	controlFile string
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func readRecord(path string) (*controlRecord, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var rec controlRecord
	if err := json.Unmarshal(data, &rec); err != nil {
		return nil, err
	}
	return &rec, nil
}

func parseClients(spec string) (map[string]Client, error) {
	out := map[string]Client{}
	for _, item := range strings.Split(spec, ",") {
		if item == "" {
			continue
		}
		f := strings.Split(item, ":")
		if len(f) != 3 || (Role(f[2]) != RoleApp && Role(f[2]) != RoleWorker) {
			return nil, fmt.Errorf("bad client %q, want user:password:app|worker", f[0])
		}
		out[f[0]] = Client{Password: f[1], Role: Role(f[2])}
	}
	return out, nil
}

func (s *server) routeOf(rec *controlRecord) (Route, error) {
	a, c := s.endpoints[rec.Active], s.endpoints[rec.Consume]
	if a == nil || c == nil {
		return Route{}, fmt.Errorf("control record names unknown backend (%q/%q)", rec.Active, rec.Consume)
	}
	return Route{Gen: rec.Gen, Active: a, Consume: c}, nil
}

func main() {
	redisx.ClientName = "statemig-proxy"
	eps := map[string]*redisx.Endpoint{}
	var order []string
	var err error
	if spec := os.Getenv("STATEMIG_ENDPOINTS"); spec != "" {
		eps, order, err = redisx.ParseEndpoints(spec)
	}
	if err != nil {
		log.Fatal(err)
	}
	s := &server{endpoints: eps, order: order, controlFile: env("STATEMIG_CONTROL_FILE", "control.json")}
	token := os.Getenv("STATEMIG_ADMIN_TOKEN")
	if token == "" {
		log.Fatal("STATEMIG_ADMIN_TOKEN is required")
	}
	clients, err := parseClients(os.Getenv("STATEMIG_CLIENTS"))
	if err != nil {
		log.Fatal(err)
	}
	s.clients = clients

	if len(order) > 0 {
		rec, err := readRecord(s.controlFile)
		if err != nil {
			name := env("STATEMIG_ACTIVE", order[0])
			rec = &controlRecord{Active: name, Consume: name}
			log.Printf("no control record (%v): starting on %s", err, name)
		}
		route, err := s.routeOf(rec)
		if err == nil {

			backends := []*redisx.Endpoint{route.Active}
			for _, n := range order {
				if eps[n] != route.Active {
					backends = append(backends, eps[n])
				}
			}
			var p *Proxy
			for i := 0; ; i++ { // backends may still be starting
				if p, err = NewProxy(route, rec.Paused, clients, backends); err == nil {
					break
				}
				if i == 30 {
					log.Fatal(err)
				}
				time.Sleep(time.Second)
			}
			s.proxy = p
		}
	}

	ln, err := net.Listen("tcp", env("STATEMIG_LISTEN", ":6379"))
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("fixed RESP proxy on %s", ln.Addr())
	go func() { log.Fatal(s.serve(ln)) }()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /status", s.status)
	mux.HandleFunc("POST /backends", s.registerBackend)
	mux.HandleFunc("POST /bind", s.bind)
	mux.HandleFunc("POST /pause", s.pause)
	mux.HandleFunc("POST /switch", s.switchRoute)
	mux.HandleFunc("POST /resume", s.resume)
	mux.HandleFunc("POST /presubscribe", s.presubscribe)
	mux.HandleFunc("POST /drain-done", s.drainDone)
	addr := env("STATEMIG_ADMIN", ":7000")
	log.Printf("admin API on %s", addr)
	admin := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.proxy == nil && r.URL.Path != "/status" && r.URL.Path != "/backends" && r.URL.Path != "/bind" {
			fail(w, fmt.Errorf("%w: proxy is not bound", ErrState))
			return
		}
		mux.ServeHTTP(w, r)
	})
	log.Fatal(http.ListenAndServe(addr, redisx.RequireToken(token, admin)))
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func fail(w http.ResponseWriter, err error) {
	code := 500
	if errors.Is(err, ErrState) {
		code = 409
	}
	writeJSON(w, code, map[string]string{"error": err.Error()})
}

func (s *server) status(w http.ResponseWriter, _ *http.Request) {
	eps := make([]*redisx.Endpoint, 0, len(s.order))
	for _, n := range s.order {
		eps = append(eps, s.endpoints[n])
	}
	var state any = map[string]any{"bound": false, "active": "", "consume": "", "gen": 0, "paused": false}
	if s.proxy != nil {
		state = s.proxy.Status()
	}
	writeJSON(w, 200, map[string]any{"proxy": state, "endpoints": eps})
}

func (s *server) serve(ln net.Listener) error {
	for {
		nc, err := ln.Accept()
		if err != nil {
			return err
		}
		s.pointerMu.RLock()
		p := s.proxy
		s.pointerMu.RUnlock()
		if p == nil {
			_, _ = nc.Write([]byte("-LOADING Redis binding is not ready\r\n"))
			_ = nc.Close()
			continue
		}
		p.ServeConn(nc)
	}
}

// Registering a backend never switches or pauses the active route. Resolved secrets stay in memory.
func (s *server) registerBackend(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name string `json:"name"`
		URL  string `json:"url"`
	}
	if json.NewDecoder(http.MaxBytesReader(w, r.Body, 65536)).Decode(&req) != nil || req.Name == "" || strings.ContainsAny(req.Name, "=,\r\n") {
		fail(w, fmt.Errorf("%w: name and Redis connection required", ErrState))
		return
	}
	ep, err := redisx.ParseEndpoint(req.Name, req.URL)
	if err != nil {
		fail(w, fmt.Errorf("%w: invalid Redis endpoint", ErrState))
		return
	}
	old := s.endpoints[req.Name]
	if old != nil && old.Same(ep) {
		writeJSON(w, 200, old)
		return
	}
	if s.proxy != nil {
		route, paused := s.proxy.Route()
		if paused || route.Active.Name != route.Consume.Name || (old != nil && (route.Active.Name == req.Name || route.Consume.Name == req.Name)) {
			fail(w, fmt.Errorf("%w: active or unfinished route cannot be replaced", ErrState))
			return
		}
		c, e := ep.DialWithin(0, 3*time.Second)
		if e != nil {
			fail(w, fmt.Errorf("%w: backend connection failed", ErrState))
			return
		}
		ct, kt, e := LoadCmdTable(c)
		_ = c.Close()
		if e != nil {
			fail(w, fmt.Errorf("%w: backend command table unavailable", ErrState))
			return
		}
		if len(ct) != len(s.proxy.cmds) {
			fail(w, fmt.Errorf("%w: incompatible backend command set", ErrState))
			return
		}
		for command, held := range ct {
			current, known := s.proxy.cmds[command]
			if !known || current != held || s.proxy.keys[command] != kt[command] {
				fail(w, fmt.Errorf("%w: incompatible backend command table", ErrState))
				return
			}
		}
	}
	if old == nil {
		s.order = append(s.order, req.Name)
	}
	s.endpoints[req.Name] = ep
	writeJSON(w, 201, ep)
}

// The API restores endpoint credentials first. The durable control record chooses the route.
func (s *server) bind(w http.ResponseWriter, _ *http.Request) {
	if s.proxy != nil {
		writeJSON(w, 200, s.proxy.Status())
		return
	}
	rec, err := readRecord(s.controlFile)
	if err != nil {
		fail(w, fmt.Errorf("%w: control record unavailable", ErrState))
		return
	}
	route, err := s.routeOf(rec)
	if err != nil {
		fail(w, err)
		return
	}
	backends := []*redisx.Endpoint{route.Active}
	if route.Consume != route.Active {
		backends = append(backends, route.Consume)
	}
	p, err := NewProxy(route, rec.Paused, s.clients, backends)
	if err != nil {
		fail(w, fmt.Errorf("%w: backend initialization failed", ErrState))
		return
	}
	s.pointerMu.Lock()
	s.proxy = p
	s.pointerMu.Unlock()
	writeJSON(w, 200, p.Status())
}

type genReq struct {
	Gen       uint64 `json:"gen"`
	MaxWaitMs int    `json:"max_wait_ms"`
}

func decode(r *http.Request) (genReq, error) {
	var req genReq
	if r.ContentLength == 0 {
		return req, nil
	}
	err := json.NewDecoder(r.Body).Decode(&req)
	return req, err
}

func (s *server) pause(w http.ResponseWriter, r *http.Request) {
	req, err := decode(r)
	if err != nil {
		fail(w, err)
		return
	}
	if req.MaxWaitMs <= 0 {
		req.MaxWaitMs = 2000
	}
	ms, err := s.proxy.Pause(time.Duration(req.MaxWaitMs) * time.Millisecond)
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"paused": true, "waited_ms": ms})
}

// recordFor loads the control record and checks it is for the requested generation.
func (s *server) recordFor(gen uint64) (Route, error) {
	rec, err := readRecord(s.controlFile)
	if err != nil {
		return Route{}, fmt.Errorf("%w: control record: %v", ErrState, err)
	}
	if rec.Gen != gen {
		return Route{}, fmt.Errorf("%w: control record is at gen %d, not %d", ErrState, rec.Gen, gen)
	}
	return s.routeOf(rec)
}

func (s *server) switchRoute(w http.ResponseWriter, r *http.Request) {
	req, err := decode(r)
	if err != nil {
		fail(w, err)
		return
	}
	if req.MaxWaitMs <= 0 || req.MaxWaitMs > 1000 {
		req.MaxWaitMs = 1000
	}
	route, err := s.recordFor(req.Gen)
	if err == nil {
		err = s.proxy.Switch(route, time.Duration(req.MaxWaitMs)*time.Millisecond)
	}
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, 200, s.proxy.Status())
}

// presubscribe subscribes every moved session on the next backend ahead of SWITCH.
func (s *server) presubscribe(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Target    string `json:"target"`
		MaxWaitMs int    `json:"max_wait_ms"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		fail(w, err)
		return
	}
	ep := s.endpoints[req.Target]
	if ep == nil {
		fail(w, fmt.Errorf("%w: unknown backend %q", ErrState, req.Target))
		return
	}
	if req.MaxWaitMs <= 0 {
		req.MaxWaitMs = 1000
	}
	t0 := time.Now()
	n, err := s.proxy.Presubscribe(ep, time.Duration(req.MaxWaitMs)*time.Millisecond)
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"sessions": n, "ms": msSince(t0)})
}

func (s *server) resume(w http.ResponseWriter, _ *http.Request) {
	ms := s.proxy.Resume()
	writeJSON(w, 200, map[string]any{"paused_ms": ms, "proxy": s.proxy.Status()})
}

func (s *server) drainDone(w http.ResponseWriter, r *http.Request) {
	req, err := decode(r)
	if err != nil {
		fail(w, err)
		return
	}
	route, err := s.recordFor(req.Gen)
	if err == nil {
		err = s.proxy.DrainDone(req.Gen, route.Consume)
	}
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, 200, s.proxy.Status())
}
