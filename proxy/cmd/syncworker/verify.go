package main

// VERIFY: compare src and dst while writes are paused.
//   delta mode:      only the keys not compared since they last changed, then a sample
//   count mode:      key counts per prefix, before the pause: it takes time in proportion
//                    to all the keys, and what writes do meanwhile is known from the marks
//   full prefixes:   key sets of both sides must match, then every value and TTL
//   sample prefixes: key counts must agree (TTL expiry may differ a little),
//                    then up to N keys taken from src are compared

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"statemig/internal/redisx"
)

const (
	verifyBatch   = 200 // keys per round trip
	verifyWorkers = 8   // round trips at once
)

type VerifyReq struct {
	Full       []string       `json:"full"`
	Sample     map[string]int `json:"sample"`
	Mode       string         `json:"mode"`        // "", "delta" or "count"
	DeadlineMs int            `json:"deadline_ms"` // 0 = none
}

var errDeadline = errors.New("verify deadline exceeded")

func deadlineOf(req VerifyReq) time.Time {
	if req.DeadlineMs <= 0 {
		return time.Time{}
	}
	return time.Now().Add(time.Duration(req.DeadlineMs) * time.Millisecond)
}

func past(deadline time.Time) bool { return !deadline.IsZero() && time.Now().After(deadline) }

// comparer compares groups of keys on several connections at once, taken from the pool.
type comparer struct {
	s        *Syncer
	deadline time.Time
	jobs     chan cmpJob
	wg       sync.WaitGroup
	mu       sync.Mutex
	bad      []string
	compared int
	err      error
}

// cmpJob is a group of keys; r, if set, gets the group's count and differences too.
type cmpJob struct {
	keys []string
	r    *prefixReport
}

func (s *Syncer) newComparer(deadline time.Time) *comparer {
	c := &comparer{s: s, deadline: deadline, jobs: make(chan cmpJob, 64)}
	srcEP, dstEP, _ := s.dir()
	for i := 0; i < verifyWorkers; i++ {
		c.wg.Add(1)
		go c.run(srcEP, dstEP)
	}
	return c
}

func (c *comparer) fail(err error) {
	c.mu.Lock()
	if c.err == nil {
		c.err = err
	}
	c.mu.Unlock()
}

func (c *comparer) failed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err != nil
}

func (c *comparer) run(srcEP, dstEP *redisx.Endpoint) {
	defer c.wg.Done()
	var src, dst *redisx.Conn
	var reused, broken bool
	release := func() {
		for _, x := range []struct {
			c  *redisx.Conn
			ep *redisx.Endpoint
		}{{src, srcEP}, {dst, dstEP}} {
			if x.c == nil {
				continue
			}
			if broken {
				x.c.Close()
			} else {
				pool.put(x.ep, x.c)
			}
		}
		src, dst, reused, broken = nil, nil, false, false
	}
	defer release()
	open := func() error {
		var r1, r2 bool
		var err error
		if src, r1, err = pool.get(srcEP); err != nil {
			return err
		}
		if dst, r2, err = pool.get(dstEP); err != nil {
			return err
		}
		reused = r1 || r2
		return nil
	}
	for job := range c.jobs { // after a failure, only drains
		if c.failed() {
			continue
		}
		if past(c.deadline) {
			c.fail(errDeadline)
			continue
		}
		if c.s.isStopped() {
			c.fail(errors.New("sync stopped"))
			continue
		}
		if src == nil || dst == nil {
			if err := open(); err != nil {
				broken = true
				release()
				c.fail(err)
				continue
			}
		}
		bad, err := c.s.compareKeys(src, dst, job.keys)
		if err != nil && reused && isNetErr(err) { // died while idle in the pool: once more, new
			broken = true
			release()
			if src, err = srcEP.Dial(0); err == nil {
				if dst, err = dstEP.Dial(0); err == nil {
					bad, err = c.s.compareKeys(src, dst, job.keys)
				}
			}
		}
		if err != nil {
			broken = true
			release()
			c.fail(err)
			continue
		}
		c.mu.Lock()
		c.compared += len(job.keys)
		c.bad = append(c.bad, bad...)
		if job.r != nil {
			job.r.Compared += len(job.keys)
			if room := 20 - len(job.r.Mismatched); room > 0 {
				job.r.Mismatched = append(job.r.Mismatched, bad[:min(room, len(bad))]...)
			}
		}
		c.mu.Unlock()
	}
}

func (c *comparer) Submit(group []string) { c.jobs <- cmpJob{keys: group} }

// SubmitAll queues keys in batches, each counted on r too.
func (c *comparer) SubmitAll(keys []string, r *prefixReport) {
	for i := 0; i < len(keys); i += verifyBatch {
		c.jobs <- cmpJob{keys: keys[i:min(i+verifyBatch, len(keys))], r: r}
	}
}

func (c *comparer) Wait() ([]string, int, error) {
	close(c.jobs)
	c.wg.Wait()
	return c.bad, c.compared, c.err
}

type prefixReport struct {
	Mode       string   `json:"mode"`
	SrcKeys    int      `json:"src_keys"`
	DstKeys    int      `json:"dst_keys"`
	Compared   int      `json:"compared"`
	Mismatched []string `json:"mismatched"`
	OnlyInSrc  []string `json:"only_in_src"`
	OnlyInDst  []string `json:"only_in_dst"`
	CountDiff  int      `json:"count_diff,omitempty"`
}

func abs(x int) int {
	if x < 0 {
		return -x
	}
	return x
}

// scanBatches hands over the keys under a prefix in groups of about n.
func scanBatches(c *redisx.Conn, prefix string, n int, fn func([]string) error) error {
	var group []string
	cursor := "0"
	for {
		v, err := c.DoStr("SCAN", cursor, "MATCH", prefix+"*", "COUNT", strconv.Itoa(n))
		if err != nil {
			return err
		}
		cursor = string(v.Arr[0].Str)
		for _, k := range v.Arr[1].Arr {
			group = append(group, string(k.Str))
		}
		if len(group) >= n || (cursor == "0" && len(group) > 0) {
			if err := fn(group); err != nil {
				return err
			}
			group = nil
		}
		if cursor == "0" {
			return nil
		}
	}
}

func scanKeys(c *redisx.Conn, prefix string, limit int) ([]string, error) {
	var out []string
	cursor := "0"
	for {
		v, err := c.DoStr("SCAN", cursor, "MATCH", prefix+"*", "COUNT", "10000")
		if err != nil {
			return nil, err
		}
		cursor = string(v.Arr[0].Str)
		for _, k := range v.Arr[1].Arr {
			out = append(out, string(k.Str))
			if limit > 0 && len(out) >= limit {
				return out, nil
			}
		}
		if cursor == "0" {
			return out, nil
		}
	}
}

type snap struct {
	typ string
	val []byte
	ttl int64
}

// snapshots reads the keys' types, values (normalised) and TTLs in two round trips.
func snapshots(c *redisx.Conn, keys []string) ([]snap, error) {
	cmds := make([][][]byte, len(keys))
	for i, k := range keys {
		cmds[i] = [][]byte{b("TYPE"), []byte(k)}
	}
	ts, err := c.Pipeline(cmds...)
	if err != nil {
		return nil, err
	}
	out := make([]snap, len(keys))
	var reads [][][]byte
	var idx []int
	for i, k := range keys {
		if err := ts[i].Err(); err != nil {
			return nil, fmt.Errorf("%s: %w", k, err)
		}
		out[i].typ = string(ts[i].Str)
		if out[i].typ == "none" {
			out[i].ttl = -2
			continue
		}
		read := readCmd(out[i].typ, []byte(k))
		if read == nil {
			return nil, fmt.Errorf("%s: %w", k, errUnsupportedType)
		}
		reads = append(reads, read, [][]byte{b("PTTL"), []byte(k)})
		idx = append(idx, i)
	}
	if len(reads) == 0 {
		return out, nil
	}
	rs, err := c.Pipeline(reads...)
	if err != nil {
		return nil, err
	}
	for j, i := range idx {
		v, t := rs[2*j], rs[2*j+1]
		for _, x := range []redisx.Value{v, t} {
			if err := x.Err(); err != nil {
				return nil, fmt.Errorf("%s: %w", keys[i], err)
			}
		}
		out[i].val, out[i].ttl = normalise(out[i].typ, v), t.Int
	}
	return out, nil
}

// both runs fn on src and dst at once: one round trip's wait, not two.
func both[T any](src, dst *redisx.Conn, fn func(*redisx.Conn) (T, error)) (T, T, error) {
	var d T
	var derr error
	done := make(chan struct{})
	go func() {
		d, derr = fn(dst)
		close(done)
	}()
	sv, err := fn(src)
	<-done
	return sv, d, errors.Join(err, derr)
}

func normalise(typ string, v redisx.Value) []byte {
	var parts [][]byte
	switch typ {
	case "string":
		return v.Str
	case "hash": // field/value pairs in any order
		for i := 0; i+1 < len(v.Arr); i += 2 {
			parts = append(parts, append(append(append([]byte(nil), v.Arr[i].Str...), 0), v.Arr[i+1].Str...))
		}
		sort.Slice(parts, func(i, j int) bool { return bytes.Compare(parts[i], parts[j]) < 0 })
	case "set":
		for _, e := range v.Arr {
			parts = append(parts, e.Str)
		}
		sort.Slice(parts, func(i, j int) bool { return bytes.Compare(parts[i], parts[j]) < 0 })
	case "stream":
		for _, e := range v.Arr {
			parts = append(parts, e.Arr[0].Str)
			for _, f := range e.Arr[1].Arr {
				parts = append(parts, f.Str)
			}
		}
	default: // list, zset: order matters
		for _, e := range v.Arr {
			parts = append(parts, e.Str)
		}
	}
	return bytes.Join(parts, []byte{1})
}

func sameSnap(a, d snap) bool {
	if a.typ != d.typ || !bytes.Equal(a.val, d.val) {
		return false
	}
	// TTL: both persistent, or both expiring within 2s of each other
	if (a.ttl < 0) != (d.ttl < 0) {
		return false
	}
	return a.ttl < 0 || abs64(a.ttl-d.ttl) <= 2000
}

// compareKeys returns the tracked keys (db and name) that differ, db by db.
func (s *Syncer) compareKeys(src, dst *redisx.Conn, keys []string) ([]string, error) {
	var bad []string
	for db, group := range groupByDB(keys) {
		if err := src.Select(db); err != nil {
			return nil, err
		}
		if err := dst.Select(db); err != nil {
			return nil, err
		}
		b, err := s.compareGroup(src, dst, group)
		if err != nil {
			return nil, err
		}
		for _, k := range b {
			bad = append(bad, dkey(db, k))
		}
	}
	return bad, nil
}

// compareGroup returns the keys of the current db that differ: both sides are read at once,
// a batch per round trip. Equal DUMP bytes settle it; anything else gets the per-type
// comparison (two more round trips for all such keys), since the same value can have a
// different encoding, and engines of different versions dump differently.
func (s *Syncer) compareGroup(src, dst *redisx.Conn, keys []string) ([]string, error) {
	check := keys
	if !s.logical.Load() {
		cmds := make([][][]byte, 0, len(keys)*2)
		for _, k := range keys {
			cmds = append(cmds, [][]byte{b("DUMP"), []byte(k)}, [][]byte{b("PTTL"), []byte(k)})
		}
		sr, dr, err := both(src, dst, func(c *redisx.Conn) ([]redisx.Value, error) { return c.Pipeline(cmds...) })
		if err != nil {
			return nil, err
		}
		check = nil
		for i, k := range keys {
			sv, dv := sr[i*2], dr[i*2]
			st, dt := sr[i*2+1].Int, dr[i*2+1].Int
			if sv.Null && dv.Null {
				continue
			}
			if !sv.IsErr() && !dv.IsErr() && !sv.Null && !dv.Null && bytes.Equal(sv.Str, dv.Str) &&
				(st < 0) == (dt < 0) && abs64(st-dt) <= 2000 {
				continue
			}
			check = append(check, k)
		}
	}
	if len(check) == 0 {
		return nil, nil
	}
	ss, ds, err := both(src, dst, func(c *redisx.Conn) ([]snap, error) { return snapshots(c, check) })
	if err != nil {
		return nil, err
	}
	var bad []string
	for i, k := range check {
		if !sameSnap(ss[i], ds[i]) {
			bad = append(bad, k)
		}
	}
	return bad, nil
}

func abs64(x int64) int64 {
	if x < 0 {
		return -x
	}
	return x
}

// verifyDelta: the keys not compared since they last changed, then a sample. Inside the write
// pause, so everything runs at once on pooled connections: the samples are scanned while the
// changed keys are compared, and each batch goes to the next free connection.
func (s *Syncer) verifyDelta(src *redisx.Conn, req VerifyReq) (map[string]*prefixReport, bool, error) {
	out := map[string]*prefixReport{}
	cmp := s.newComparer(deadlineOf(req))
	var sub sync.WaitGroup
	var mu sync.Mutex
	var scanErr error
	delta := s.deltaKeys()
	r := &prefixReport{Mode: "delta", SrcKeys: len(delta)}
	out["delta"] = r
	sub.Add(1)
	go func() { defer sub.Done(); cmp.SubmitAll(delta, r) }()
	var dbs []int
	for p, n := range req.Sample {
		if _, ok := s.presampled(p, n); !ok { // only needed to scan
			var err error
			if dbs, err = dbsOf(src); err != nil {
				sub.Wait()
				cmp.Wait()
				return nil, false, err
			}
			break
		}
	}
	srcEP, _, _ := s.dir()
	for p, n := range req.Sample {
		pr := &prefixReport{Mode: fmt.Sprintf("sample %d", n)}
		out[p] = pr
		sub.Add(1)
		go func() {
			defer sub.Done()
			sample, ok := s.presampled(p, n)
			if !ok {
				var err error
				if sample, err = sampleKeys(srcEP, dbs, p, n); err != nil {
					mu.Lock()
					scanErr = err
					mu.Unlock()
					return
				}
			}
			cmp.SubmitAll(sample, pr)
		}()
	}
	sub.Wait()
	bad, _, err := cmp.Wait()
	if err == nil {
		err = scanErr
	}
	if err != nil {
		return nil, false, err
	}
	return out, len(bad) == 0, nil
}

// sampleKeys takes up to n keys under prefix p from src, from the dbs in turn.
func sampleKeys(srcEP *redisx.Endpoint, dbs []int, p string, n int) ([]string, error) {
	var sample []string
	err := pool.withConn(srcEP, func(c *redisx.Conn) error {
		sample = sample[:0]
		for _, db := range dbs {
			if len(sample) >= n {
				break
			}
			if err := c.Select(db); err != nil {
				return err
			}
			ks, err := scanKeys(c, p, n-len(sample))
			if err != nil {
				return err
			}
			for _, k := range ks {
				sample = append(sample, dkey(db, k))
			}
		}
		return nil
	})
	return sample, err
}

// verifyCount compares key counts while writes continue. Writes can move the counts apart
// by at most what was marked or waiting meanwhile. Keys appearing on dst after this and
// before the pause go unnoticed: the same accepted risk as a value changed on dst alone.
func (s *Syncer) verifyCount(req VerifyReq) (map[string]*prefixReport, bool, error) {
	srcEP, dstEP, _ := s.dir()
	before := s.marked.Load()
	// Counted per db and prefix: a key moved between dbs must not cancel out in a total.
	count := func(ep *redisx.Endpoint) (map[string]int, error) {
		c, err := ep.Dial(0)
		if err != nil {
			return nil, err
		}
		defer c.Close()
		dbs, err := dbsOf(c)
		if err != nil {
			return nil, err
		}
		n := map[string]int{}
		err = s.scanDBs(c, dbs, func(db int, keys []string) error {
			for _, k := range keys {
				for p := range req.Sample {
					if strings.HasPrefix(k, p) {
						n[prefixLabel(db, p)]++
					}
				}
			}
			return nil
		})
		return n, err
	}
	var wg sync.WaitGroup
	var srcN, dstN map[string]int
	var srcErr, dstErr error
	wg.Add(2)
	go func() {
		defer wg.Done()
		srcN, srcErr = count(srcEP)
	}()
	go func() {
		defer wg.Done()
		dstN, dstErr = count(dstEP)
	}()
	wg.Wait()
	if err := errors.Join(srcErr, dstErr); err != nil {
		return nil, false, err
	}
	out := map[string]*prefixReport{}
	for p := range req.Sample {
		out[p] = &prefixReport{Mode: "count"}
	}
	for _, n := range []map[string]int{srcN, dstN} {
		for label := range n {
			if out[label] == nil {
				out[label] = &prefixReport{Mode: "count"}
			}
		}
	}
	for label, pr := range out {
		pr.SrcKeys, pr.DstKeys = srcN[label], dstN[label]
	}
	waiting, _ := s.Backlog()
	slack := int(s.marked.Load()-before) + waiting
	ok := true
	for _, pr := range out {
		if abs(pr.SrcKeys-pr.DstKeys) > max(2, pr.SrcKeys/1000)+slack {
			pr.CountDiff = pr.DstKeys - pr.SrcKeys
			ok = false
		}
	}
	return out, ok, nil
}

func (s *Syncer) Verify(req VerifyReq) (map[string]*prefixReport, bool, error) {
	if req.Mode == "count" {
		return s.verifyCount(req)
	}
	deadline := deadlineOf(req)
	srcEP, dstEP, _ := s.dir()
	if req.Mode == "delta" {
		var out map[string]*prefixReport
		var ok bool
		err := pool.withConn(srcEP, func(src *redisx.Conn) error {
			var err error
			out, ok, err = s.verifyDelta(src, req)
			return err
		})
		return out, ok, err
	}
	src, err := srcEP.Dial(0)
	if err != nil {
		return nil, false, err
	}
	defer src.Close()
	dst, err := dstEP.Dial(0)
	if err != nil {
		return nil, false, err
	}
	defer dst.Close()

	out := map[string]*prefixReport{}
	ok := true
	add := func(xs []string, x string) []string {
		if len(xs) < 20 {
			xs = append(xs, x)
		}
		return xs
	}
	dbs, err := dbsOf(src, dst)
	if err != nil {
		return nil, false, err
	}
	for _, db := range dbs {
		if err := src.Select(db); err != nil {
			return nil, false, err
		}
		if err := dst.Select(db); err != nil {
			return nil, false, err
		}
		if err := s.verifyFull(src, dst, req, db, deadline, out, &ok, add); err != nil {
			return nil, false, err
		}
	}
	return out, ok, nil
}

// prefixLabel names a report row: the prefix for db 0, "db3/session:" for another db.
func prefixLabel(db int, p string) string {
	if db == 0 {
		return p
	}
	return fmt.Sprintf("db%d/%s", db, p)
}

// verifyFull is the full-mode VERIFY of one db; both connections are on it.
func (s *Syncer) verifyFull(src, dst *redisx.Conn, req VerifyReq, db int, deadline time.Time,
	out map[string]*prefixReport, ok *bool, add func([]string, string) []string) error {
	for _, p := range req.Full {
		r := &prefixReport{Mode: "full"}
		sk, err := scanKeys(src, p, 0)
		if err != nil {
			return err
		}
		dk, err := scanKeys(dst, p, 0)
		if err != nil {
			return err
		}
		r.SrcKeys, r.DstKeys = len(sk), len(dk)
		inSrc := map[string]bool{}
		for _, k := range sk {
			inSrc[k] = true
		}
		for _, k := range dk {
			if !inSrc[k] {
				r.OnlyInDst = add(r.OnlyInDst, dkey(db, k))
				*ok = false
			}
		}
		for i := 0; i < len(sk); i += verifyBatch {
			if past(deadline) {
				return errDeadline
			}
			group := sk[i:min(i+verifyBatch, len(sk))]
			bad, err := s.compareGroup(src, dst, group)
			if err != nil {
				return err
			}
			r.Compared += len(group)
			for _, k := range bad {
				r.Mismatched = add(r.Mismatched, dkey(db, k))
				*ok = false
			}
		}
		out[prefixLabel(db, p)] = r
	}
	for p, n := range req.Sample {
		r := &prefixReport{Mode: fmt.Sprintf("sample %d", n)}
		sk, err := scanKeys(src, p, 0)
		if err != nil {
			return err
		}
		dk, err := scanKeys(dst, p, 0)
		if err != nil {
			return err
		}
		r.SrcKeys, r.DstKeys = len(sk), len(dk)
		if diff, tol := abs(len(sk)-len(dk)), max(2, len(sk)/100); diff > tol {
			r.CountDiff = len(dk) - len(sk)
			*ok = false
		}
		if len(sk) > n {
			sk = sk[:n]
		}
		for i := 0; i < len(sk); i += verifyBatch {
			group := sk[i:min(i+verifyBatch, len(sk))]
			bad, err := s.compareGroup(src, dst, group)
			if err != nil {
				return err
			}
			r.Compared += len(group)
			for _, k := range bad {
				r.Mismatched = add(r.Mismatched, dkey(db, k))
				*ok = false
			}
		}
		out[prefixLabel(db, p)] = r
	}
	return nil
}
