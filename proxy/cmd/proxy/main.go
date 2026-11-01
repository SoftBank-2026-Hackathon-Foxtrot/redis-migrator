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
	proxy       *Proxy
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
	eps, order, err := redisx.ParseEndpoints(env("STATEMIG_ENDPOINTS", "local=redis://localhost:6379"))
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

	rec, err := readRecord(s.controlFile)
	if err != nil {
		name := env("STATEMIG_ACTIVE", order[0])
		rec = &controlRecord{Active: name, Consume: name}
		log.Printf("no control record (%v): starting on %s", err, name)
	}
	route, err := s.routeOf(rec)
	if err != nil {
		log.Fatal(err)
	}

	var p *Proxy
	for i := 0; ; i++ { // backends may still be starting
		if p, err = NewProxy(route, rec.Paused, clients); err == nil {
			break
		}
		if i == 30 {
			log.Fatal(err)
		}
		time.Sleep(time.Second)
	}
	s.proxy = p

	ln, err := net.Listen("tcp", env("STATEMIG_LISTEN", ":6379"))
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("RESP proxy on %s gen=%d active=%s consume=%s paused=%v", ln.Addr(), route.Gen, route.Active.Name,
		route.Consume.Name, rec.Paused)
	go func() { log.Fatal(p.Serve(ln)) }()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /status", s.status)
	mux.HandleFunc("POST /pause", s.pause)
	mux.HandleFunc("POST /switch", s.switchRoute)
	mux.HandleFunc("POST /resume", s.resume)
	mux.HandleFunc("POST /drain-done", s.drainDone)
	addr := env("STATEMIG_ADMIN", ":7000")
	log.Printf("admin API on %s", addr)
	log.Fatal(http.ListenAndServe(addr, redisx.RequireToken(token, mux)))
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
	writeJSON(w, 200, map[string]any{"proxy": s.proxy.Status(), "endpoints": eps})
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
	route, err := s.recordFor(req.Gen)
	if err == nil {
		err = s.proxy.Switch(route)
	}
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, 200, s.proxy.Status())
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
