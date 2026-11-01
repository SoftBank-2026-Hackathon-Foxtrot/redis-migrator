package main

// statemig-syncworker: keeps the target Redis in step with the source during a
// migration, and the old source in step with the new one afterwards. It is not
// on the request path; it talks to both Redis servers directly.
//
// env:
//   STATEMIG_ENDPOINTS  aws=redis://admin:pw@redis-aws:6379,gcp=... (migration ACL user)
//   STATEMIG_ADMIN_TOKEN  bearer token required on the admin API
//   STATEMIG_ADMIN      admin HTTP addr (default :7100)

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"statemig/internal/redisx"
)

type server struct {
	endpoints map[string]*redisx.Endpoint
	mu        sync.Mutex
	sy        *Syncer
}

func main() {
	spec := os.Getenv("STATEMIG_ENDPOINTS")
	if spec == "" {
		spec = "local=redis://localhost:6379"
	}
	eps, _, err := redisx.ParseEndpoints(spec)
	if err != nil {
		log.Fatal(err)
	}
	token := os.Getenv("STATEMIG_ADMIN_TOKEN")
	if token == "" {
		log.Fatal("STATEMIG_ADMIN_TOKEN is required")
	}
	s := &server{endpoints: eps}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /status", s.status)
	mux.HandleFunc("POST /start", s.start)
	mux.HandleFunc("POST /precompare", s.precompare)
	mux.HandleFunc("POST /reconcile", s.reconcile)
	mux.HandleFunc("POST /verify", s.verify)
	mux.HandleFunc("POST /reverse", s.reverse)
	mux.HandleFunc("POST /stop", s.stop)
	addr := os.Getenv("STATEMIG_ADMIN")
	if addr == "" {
		addr = ":7100"
	}
	log.Printf("sync worker admin API on %s", addr)
	log.Fatal(http.ListenAndServe(addr, redisx.RequireToken(token, mux)))
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func fail(w http.ResponseWriter, code int, err error) {
	writeJSON(w, code, map[string]string{"error": err.Error()})
}

func (s *server) current(w http.ResponseWriter) *Syncer {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sy == nil {
		fail(w, 409, fmt.Errorf("no sync running"))
	}
	return s.sy
}

func (s *server) status(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	sy := s.sy
	s.mu.Unlock()
	if sy == nil {
		writeJSON(w, 200, map[string]any{"running": false})
		return
	}
	writeJSON(w, 200, map[string]any{"running": true, "sync": sy.Status()})
}

type startReq struct {
	Src     string   `json:"src"`
	Dst     string   `json:"dst"`
	Include []string `json:"include"`
	Workers int      `json:"workers"`
}

func (s *server) start(w http.ResponseWriter, r *http.Request) {
	var req startReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		fail(w, 400, err)
		return
	}
	src, dst := s.endpoints[req.Src], s.endpoints[req.Dst]
	if src == nil || dst == nil || src == dst || len(req.Include) == 0 {
		fail(w, 400, fmt.Errorf("need distinct known src/dst and at least one include prefix"))
		return
	}
	if req.Workers <= 0 {
		req.Workers = 16
	}
	s.mu.Lock()
	old := s.sy
	s.sy = nil
	s.mu.Unlock()
	if old != nil {
		old.Stop()
	}
	sy := NewSyncer(src, dst, req.Include, req.Workers)
	if err := sy.Start(10 * time.Second); err != nil {
		sy.Stop()
		fail(w, 500, err)
		return
	}
	s.mu.Lock()
	s.sy = sy
	s.mu.Unlock()
	log.Printf("sync %s → %s started for %v", src.Name, dst.Name, req.Include)
	writeJSON(w, 201, sy.Status())
}

type deadlineReq struct {
	DeadlineMs int    `json:"deadline_ms"`
	Src        string `json:"src"`
	Mode       string `json:"mode"` // reconcile: "" or "delta"
}

func parseDeadline(r *http.Request) (deadlineReq, time.Time) {
	var req deadlineReq
	json.NewDecoder(r.Body).Decode(&req)
	if req.DeadlineMs <= 0 {
		req.DeadlineMs = 5000
	}
	return req, time.Now().Add(time.Duration(req.DeadlineMs) * time.Millisecond)
}

func deadline(r *http.Request) time.Time {
	_, d := parseDeadline(r)
	return d
}

func (s *server) reconcile(w http.ResponseWriter, r *http.Request) {
	sy := s.current(w)
	if sy == nil {
		return
	}
	t0 := time.Now()
	req, d := parseDeadline(r)
	var n int
	var err error
	if req.Mode == "delta" {
		n, err = sy.ReconcileDelta(d)
	} else {
		n, err = sy.Reconcile(d)
	}
	if err != nil {
		fail(w, 409, err)
		return
	}
	writeJSON(w, 200, map[string]any{"scanned": n, "ms": float64(time.Since(t0).Microseconds()) / 1000})
}

func (s *server) precompare(w http.ResponseWriter, r *http.Request) {
	sy := s.current(w)
	if sy == nil {
		return
	}
	res, err := sy.Precompare(deadline(r))
	if err != nil {
		fail(w, 409, err)
		return
	}
	writeJSON(w, 200, res)
}

func (s *server) verify(w http.ResponseWriter, r *http.Request) {
	sy := s.current(w)
	if sy == nil {
		return
	}
	var req VerifyReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		fail(w, 400, err)
		return
	}
	t0 := time.Now()
	rep, ok, err := sy.Verify(req)
	if err != nil {
		fail(w, 500, err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": ok, "prefixes": rep, "ms": float64(time.Since(t0).Microseconds()) / 1000})
}

func (s *server) reverse(w http.ResponseWriter, r *http.Request) {
	sy := s.current(w)
	if sy == nil {
		return
	}
	req, d := parseDeadline(r)
	if req.Src == "" {
		fail(w, 400, fmt.Errorf("src is required"))
		return
	}
	if err := sy.Reverse(req.Src, d); err != nil {
		fail(w, 409, err)
		return
	}
	log.Printf("reversed: now %s → %s", sy.Status()["src"], sy.Status()["dst"])
	writeJSON(w, 200, sy.Status())
}

func (s *server) stop(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	sy := s.sy
	s.sy = nil
	s.mu.Unlock()
	if sy == nil {
		writeJSON(w, 200, map[string]any{"running": false})
		return
	}
	st := sy.Status()
	sy.Stop()
	writeJSON(w, 200, map[string]any{"stopped": st})
}
