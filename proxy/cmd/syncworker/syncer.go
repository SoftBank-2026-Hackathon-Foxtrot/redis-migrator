package main

// One-directional sync src → dst for the keys under the included prefixes.
//
//   - Change capture: keyspace notifications (PSUBSCRIBE __keyspace@0__:*) on src.
//     A notification only carries the key name; the worker always reads the
//     key's *current* value from src, so handling a notification twice is harmless.
//   - Start order: subscribe (confirmed) → enqueue a full scan of both sides.
//     Any write after the subscription is confirmed produces a notification,
//     so no change can fall between the scan and the subscription.
//   - Per-key ordering: keys are sharded by hash and each shard has one worker,
//     so two copies of the same key never run concurrently and the last
//     processed change always decides the result.
//   - Lost notifications (Pub/Sub has no replay): after every reconnect, and in
//     the final reconcile, both sides are scanned. Keys present in src are
//     copied, keys only present in dst are deleted.
//   - Source restarts: a new run_id on src means its data may be gone, and copying
//     from an empty src would delete everything on dst. Before any connection to
//     src is used, a restart with too few keys left halts the sync for good: dst
//     stays as it was and the operator decides.
//   - Precompare (delta mode): both sides are compared while writes continue and
//     every key marked from then on is remembered. As long as the subscription
//     never dropped, a key outside that set still equals what was compared, so the
//     cutover only has to finish and check those keys. A barrier key written after
//     the pause proves every earlier notification has arrived.
//   - Rolling compare: the remembered keys are compared in the background too, so
//     the pause only checks what changed in the last moments, not everything that
//     changed since the precompare.
//   - Lag: a heartbeat key written to src and timed until its notification comes
//     back, which also counts what is still in flight. The backlog alone can't tell
//     a healthy high write rate from a sync falling behind.

import (
	"errors"
	"hash/fnv"
	"log"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"statemig/internal/redisx"
)

const (
	notifyPattern = "__keyspace@*__:*" // every db, on one connection: the events of all dbs arrive in order
	barrierKey    = "__statemig:barrier"
	heartbeatKey = "__statemig:heartbeat"
	copyBatch    = 100 // keys per round trip, per worker
	minKeptPct   = 90  // a restarted src must still hold this share of dst's keys
	countBatch   = 1000

	monitorEvery   = 250 * time.Millisecond
	rollEvery      = time.Second
	rollTimeout    = 30 * time.Second
	settledBacklog = 1000 // the precompare starts once the first copy is about done
	healthyLagMs   = 1000 // heartbeat round trip, and time to copy the backlog
	lostBeatMs     = 5000 // a heartbeat this late is replaced by a new one
)

type shard struct {
	mu      sync.Mutex
	cond    *sync.Cond
	dirty   map[string]int64 // key → unix ms when it first became dirty
	copying int
}

type Syncer struct {
	include []string

	mu       sync.Mutex
	src, dst *redisx.Endpoint
	dirEpoch uint64 // bumped on reverse; workers reopen their connections
	stopped  bool
	sub      *redisx.Conn // current subscription connection

	shards []*shard
	wg     sync.WaitGroup

	logical                                               atomic.Bool // DUMP payload rejected by dst → per-type copy
	subscribed                                            atomic.Bool
	copied, deleted, errs, marked, notifications, resyncs atomic.Int64
	lastErr                                               atomic.Value
	lastFullScan                                          atomic.Int64 // unix ms
	confirmed                                             chan struct{}

	deltaMu        sync.Mutex
	tracking       bool                // remember marked keys since the last precompare
	delta          map[string]struct{} // keys marked since the last precompare started
	comparedResync atomic.Int64        // resyncs at the last finished precompare; -1 = none
	comparing      atomic.Bool
	lastCompare    atomic.Value // map[string]any
	barrierMu      sync.Mutex
	barrierSeen    chan struct{}

	guardMu sync.Mutex
	srcRun  string       // run_id of src at the first connection; "" = not seen yet
	halted  atomic.Value // string; why the sync stopped for good, "" = running

	rolling     map[string]struct{} // keys being compared right now; guarded by deltaMu
	rollGen     uint64              // bumped when tracking restarts; guarded by deltaMu
	rollBusy    atomic.Bool
	scanning    atomic.Bool  // the first scan is still enqueuing keys
	compareRate atomic.Int64 // keys per second at the last compare

	beatSent, beatLag atomic.Int64 // unix ms of the heartbeat in flight (0 = none), last round trip in ms
	stableSince       atomic.Int64 // unix ms since the sync has kept up (0 = it has not)
	copyRate          atomic.Int64 // keys per second
}

func NewSyncer(src, dst *redisx.Endpoint, include []string, workers int) *Syncer {
	s := &Syncer{include: include, src: src, dst: dst}
	s.lastErr.Store("")
	s.halted.Store("")
	s.comparedResync.Store(-1)
	s.lastCompare.Store(map[string]any{})
	for i := 0; i < workers; i++ {
		sh := &shard{dirty: map[string]int64{}}
		sh.cond = sync.NewCond(&sh.mu)
		s.shards = append(s.shards, sh)
	}
	return s
}

func (s *Syncer) included(key string) bool {
	for _, p := range s.include {
		if strings.HasPrefix(key, p) {
			return true
		}
	}
	return false
}

// A tracked key carries its db, which a migration keeps: "3:session:abc" is session:abc in db 3
// on both sides. Barrier and heartbeat live in db 0.
func dkey(db int, key string) string { return strconv.Itoa(db) + ":" + key }

func splitKey(k string) (int, string) {
	i := strings.IndexByte(k, ':')
	db, _ := strconv.Atoi(k[:i])
	return db, k[i+1:]
}

// parseNotify reads "__keyspace@3__:session:abc".
func parseNotify(channel string) (int, string, bool) {
	rest, ok := strings.CutPrefix(channel, "__keyspace@")
	if !ok {
		return 0, "", false
	}
	i := strings.Index(rest, "__:")
	if i < 0 {
		return 0, "", false
	}
	db, err := strconv.Atoi(rest[:i])
	if err != nil {
		return 0, "", false
	}
	return db, rest[i+3:], true
}

// keyspaceDBs lists the dbs that hold keys, from INFO keyspace.
func keyspaceDBs(c *redisx.Conn) ([]int, error) {
	v, err := c.DoStr("INFO", "keyspace")
	if err != nil {
		return nil, err
	}
	var out []int
	for _, line := range strings.Split(string(v.Str), "\n") {
		name, _, ok := strings.Cut(strings.TrimSpace(line), ":")
		if n, err := strconv.Atoi(strings.TrimPrefix(name, "db")); ok && strings.HasPrefix(name, "db") && err == nil {
			out = append(out, n)
		}
	}
	return out, nil
}

// dbsOf is every db that holds keys on any of the connections, db 0 always included.
func dbsOf(conns ...*redisx.Conn) ([]int, error) {
	seen := map[int]bool{0: true}
	for _, c := range conns {
		dbs, err := keyspaceDBs(c)
		if err != nil {
			return nil, err
		}
		for _, db := range dbs {
			seen[db] = true
		}
	}
	out := make([]int, 0, len(seen))
	for db := range seen {
		out = append(out, db)
	}
	sort.Ints(out)
	return out, nil
}

// groupByDB splits tracked keys into raw key names per db.
func groupByDB(keys []string) map[int][]string {
	out := map[int][]string{}
	for _, k := range keys {
		db, key := splitKey(k)
		out[db] = append(out[db], key)
	}
	return out
}

// scanDBs runs scanIncluded on each db in turn; the connection is left on the last one.
func (s *Syncer) scanDBs(c *redisx.Conn, dbs []int, fn func(int, []string) error) error {
	for _, db := range dbs {
		if err := c.Select(db); err != nil {
			return err
		}
		if err := s.scanIncluded(c, func(keys []string) error { return fn(db, keys) }); err != nil {
			return err
		}
	}
	return nil
}

func (s *Syncer) shardOf(key string) *shard {
	h := fnv.New32a()
	h.Write([]byte(key))
	return s.shards[int(h.Sum32())%len(s.shards)]
}

func (s *Syncer) Mark(key string) {
	s.deltaMu.Lock()
	if s.tracking {
		s.delta[key] = struct{}{}
	}
	s.deltaMu.Unlock()
	sh := s.shardOf(key)
	sh.mu.Lock()
	if _, ok := sh.dirty[key]; !ok {
		sh.dirty[key] = time.Now().UnixMilli()
	}
	sh.mu.Unlock()
	sh.cond.Signal()
	s.marked.Add(1)
}

func (s *Syncer) dir() (*redisx.Endpoint, *redisx.Endpoint, uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.src, s.dst, s.dirEpoch
}

func (s *Syncer) haltReason() string { return s.halted.Load().(string) }

func (s *Syncer) halt(reason string) {
	s.halted.Store(reason)
	s.lastErr.Store(reason)
	log.Printf("sync halted: %s", reason)
}

func (s *Syncer) haltedErr() error {
	if r := s.haltReason(); r != "" {
		return errors.New("sync halted: " + r)
	}
	return nil
}

func runID(c *redisx.Conn) (string, error) {
	v, err := c.DoStr("INFO", "server")
	if err != nil {
		return "", err
	}
	for _, line := range strings.Split(string(v.Str), "\n") {
		if id, ok := strings.CutPrefix(line, "run_id:"); ok {
			return strings.TrimSpace(id), nil
		}
	}
	return "", nil
}

// scanIncluded walks the keyspace once and filters here. SCAN MATCH per prefix would walk
// all of it once per prefix, a round trip per page.
func (s *Syncer) scanIncluded(c *redisx.Conn, fn func([]string) error) error {
	cursor := "0"
	for {
		v, err := c.DoStr("SCAN", cursor, "COUNT", strconv.Itoa(countBatch))
		if err != nil {
			return err
		}
		cursor = string(v.Arr[0].Str)
		keys := make([]string, 0, len(v.Arr[1].Arr))
		for _, k := range v.Arr[1].Arr {
			if s.included(string(k.Str)) {
				keys = append(keys, string(k.Str))
			}
		}
		if len(keys) > 0 {
			if err := fn(keys); err != nil {
				return err
			}
		}
		if cursor == "0" {
			return nil
		}
	}
}

// countKeys counts the included keys of every db.
func (s *Syncer) countKeys(c *redisx.Conn) (int, error) {
	dbs, err := dbsOf(c)
	if err != nil {
		return 0, err
	}
	n := 0
	err = s.scanDBs(c, dbs, func(_ int, keys []string) error {
		n += len(keys)
		return nil
	})
	return n, err
}

// lostKeys says why src can't be trusted against dst, or "" if it can.
func (s *Syncer) lostKeys(src *redisx.Conn, dstEP *redisx.Endpoint) (string, error) {
	dst, err := dstEP.Dial(0)
	if err != nil {
		return "", err
	}
	defer dst.Close()
	srcN, err := s.countKeys(src)
	if err != nil {
		return "", err
	}
	dstN, err := s.countKeys(dst)
	if err != nil {
		return "", err
	}
	if srcN*100 < dstN*minKeptPct {
		return "source has " + strconv.Itoa(srcN) + " keys, target " + strconv.Itoa(dstN), nil
	}
	return "", nil
}

// checkSource runs on every new connection to src, before it is used for anything.
func (s *Syncer) checkSource(c *redisx.Conn, ep *redisx.Endpoint) error {
	s.guardMu.Lock()
	defer s.guardMu.Unlock()
	if err := s.haltedErr(); err != nil {
		return err
	}
	src, dst, _ := s.dir()
	if src != ep {
		return nil
	}
	run, err := runID(c)
	if err != nil {
		return err
	}
	if s.srcRun == "" {
		s.srcRun = run
		return nil
	}
	if run == "" || run == s.srcRun {
		return nil
	}
	why, err := s.lostKeys(c, dst)
	if err != nil {
		return err
	}
	if why != "" {
		s.halt(src.Name + " restarted and lost data: " + why)
		return s.haltedErr()
	}
	if err := restoreNotifications(c); err != nil {
		s.halt(src.Name + " restarted without keyspace notifications: " + err.Error())
		return s.haltedErr()
	}
	log.Printf("%s restarted with its data: continuing", src.Name)
	s.srcRun = run
	return nil
}

// restoreNotifications turns notifications back on after a restart that undid them.
// A managed Redis refuses CONFIG; there they are set by IaC and survive restarts.
func restoreNotifications(c *redisx.Conn) error {
	v, err := c.DoStr("CONFIG", "GET", "notify-keyspace-events")
	if err != nil || len(v.Arr) < 2 {
		return nil
	}
	if cur := string(v.Arr[1].Str); strings.Contains(cur, "K") && (strings.Contains(cur, "A") || strings.Contains(cur, "g")) {
		return nil
	}
	_, err = c.DoStr("CONFIG", "SET", "notify-keyspace-events", "KA")
	return err
}

// Start subscribes to src, waits for the subscription to be confirmed, then
// enqueues a full two-sided scan. With expectSynced (a sync picked up again after a
// Sync Worker restart) it refuses a src that has lost what dst still holds.
func (s *Syncer) Start(timeout time.Duration, expectSynced bool) error {
	if expectSynced {
		c, err := s.src.Dial(0)
		if err != nil {
			return err
		}
		why, err := s.lostKeys(c, s.dst)
		c.Close()
		if err != nil {
			return err
		}
		if why != "" {
			return errors.New("refusing to resume: " + why)
		}
	}
	for _, sh := range s.shards {
		s.wg.Add(1)
		go s.worker(sh)
	}
	s.wg.Add(1)
	go s.monitor()
	if err := s.subscribe(timeout); err != nil {
		return err
	}
	s.scanning.Store(true)
	s.wg.Add(1)
	go s.firstScan() // can outlast any reasonable request timeout on a big keyspace
	return nil
}

func (s *Syncer) firstScan() {
	defer s.wg.Done()
	defer s.scanning.Store(false)
	for !s.isStopped() && s.haltReason() == "" {
		_, err := s.FullScan()
		if err == nil {
			return
		}
		s.lastErr.Store("first scan: " + err.Error())
		time.Sleep(time.Second)
	}
}

func (s *Syncer) subscribe(timeout time.Duration) error {
	confirmed := make(chan struct{})
	s.mu.Lock()
	s.confirmed = confirmed
	s.mu.Unlock()
	src, _, epoch := s.dir()
	s.wg.Add(1)
	go s.subscriber(src, epoch, confirmed)
	return waitConfirmed(confirmed, timeout)
}

func waitConfirmed(confirmed chan struct{}, timeout time.Duration) error {
	select {
	case <-confirmed:
		return nil
	case <-time.After(timeout):
		return errors.New("keyspace subscription not confirmed in time")
	}
}

func (s *Syncer) subscriber(src *redisx.Endpoint, epoch uint64, confirmed chan struct{}) {
	defer s.wg.Done()
	first := true
	for {
		s.mu.Lock()
		if s.stopped || s.dirEpoch != epoch {
			s.mu.Unlock()
			return
		}
		s.mu.Unlock()
		c, err := src.Dial(0)
		if err == nil {
			err = s.checkSource(c, src)
		}
		if err == nil {
			redisx.WriteCommand(c.Writer(), redisx.B("PSUBSCRIBE"), redisx.B(notifyPattern))
			if err = c.Writer().Flush(); err == nil {
				var v redisx.Value
				if v, err = redisx.ReadValue(c.Reader()); err == nil && v.IsErr() { // ["psubscribe", pattern, 1]
					err = v.Err()
				}
			}
		}
		if err != nil {
			if c != nil {
				c.Close()
			}
			if s.haltReason() != "" {
				return
			}
			s.lastErr.Store("subscribe " + src.Name + ": " + err.Error())
			time.Sleep(500 * time.Millisecond)
			continue
		}
		s.mu.Lock()
		if s.stopped || s.dirEpoch != epoch {
			s.mu.Unlock()
			c.Close()
			return
		}
		s.sub = c
		s.mu.Unlock()
		s.subscribed.Store(true)
		if first {
			first = false
			close(confirmed)
		} else {
			// notifications sent while we were disconnected are gone
			s.resyncs.Add(1)
			log.Printf("resubscribed to %s: full two-sided rescan", src.Name)
			go s.FullScan()
		}
		for {
			v, err := redisx.ReadValue(c.Reader())
			if err != nil {
				break
			}
			if len(v.Arr) == 4 && string(v.Arr[0].Str) == "pmessage" {
				db, key, ok := parseNotify(string(v.Arr[2].Str))
				if !ok {
					continue
				}
				s.notifications.Add(1)
				if db == 0 && key == barrierKey {
					s.barrierArrived()
				} else if db == 0 && key == heartbeatKey {
					s.beatArrived()
				} else if s.included(key) {
					s.Mark(dkey(db, key))
				}
			}
		}
		s.subscribed.Store(false)
		c.Close()
	}
}

func (s *Syncer) closeSub() {
	s.mu.Lock()
	if s.sub != nil {
		s.sub.Close()
		s.sub = nil
	}
	s.mu.Unlock()
}

// FullScan enqueues every included key of src, and the keys only dst has (to delete them).
func (s *Syncer) FullScan() (int, error) {
	srcEP, dstEP, _ := s.dir()
	src, err := srcEP.Dial(0)
	if err != nil {
		return 0, err
	}
	defer src.Close()
	dst, err := dstEP.Dial(0)
	if err != nil {
		return 0, err
	}
	defer dst.Close()
	dbs, err := dbsOf(src, dst)
	if err != nil {
		return 0, err
	}
	n := 0
	err = s.scanDBs(src, dbs, func(db int, keys []string) error {
		for _, k := range keys {
			s.Mark(dkey(db, k))
		}
		n += len(keys)
		return nil
	})
	if err != nil {
		return n, err
	}
	err = s.scanDBs(dst, dbs, func(db int, keys []string) error {
		only, err := missingOn(src, db, keys)
		for _, k := range only {
			s.Mark(dkey(db, k))
		}
		n += len(only)
		return err
	})
	if err != nil {
		return n, err
	}
	s.lastFullScan.Store(time.Now().UnixMilli())
	return n, nil
}

// missingOn returns the keys db on c does not have.
func missingOn(c *redisx.Conn, db int, keys []string) ([]string, error) {
	if err := c.Select(db); err != nil {
		return nil, err
	}
	cmds := make([][][]byte, len(keys))
	for i, k := range keys {
		cmds[i] = [][]byte{b("EXISTS"), []byte(k)}
	}
	r, err := c.Pipeline(cmds...)
	if err != nil {
		return nil, err
	}
	var out []string
	for i, v := range r {
		if v.Int == 0 {
			out = append(out, keys[i])
		}
	}
	return out, nil
}

func (s *Syncer) Backlog() (int, int64) {
	n := 0
	oldest := int64(0)
	for _, sh := range s.shards {
		sh.mu.Lock()
		n += len(sh.dirty) + sh.copying
		for _, t := range sh.dirty {
			if oldest == 0 || t < oldest {
				oldest = t
			}
		}
		sh.mu.Unlock()
	}
	age := int64(0)
	if oldest > 0 {
		age = time.Now().UnixMilli() - oldest
	}
	return n, age
}

func (s *Syncer) Drain(deadline time.Time) bool {
	for {
		if n, _ := s.Backlog(); n == 0 {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(200 * time.Microsecond)
	}
}

// Reconcile is the final reconcile: drain, scan both sides, drain again.
// Called while writes are paused, so afterwards dst equals src for every included key.
func (s *Syncer) Reconcile(deadline time.Time) (int, error) {
	if err := s.haltedErr(); err != nil {
		return 0, err
	}
	if !s.Drain(deadline) {
		n, _ := s.Backlog()
		return 0, errors.New("drain timeout (backlog " + strconv.Itoa(n) + ")")
	}
	n, err := s.FullScan()
	if err != nil {
		return n, err
	}
	if !s.Drain(deadline) {
		b, _ := s.Backlog()
		return n, errors.New("drain timeout after scan (backlog " + strconv.Itoa(b) + ")")
	}
	return n, nil
}

// Reverse makes newSrc the source of changes and waits until the subscription
// on it is confirmed. Safe to call again after a timeout: the direction is
// already flipped then and the subscriber is still retrying, so it only waits.
func (s *Syncer) Reverse(newSrc string, deadline time.Time) error {
	s.mu.Lock()
	cur, confirmed := s.src, s.confirmed
	s.mu.Unlock()
	if cur.Name == newSrc {
		return waitConfirmed(confirmed, time.Until(deadline))
	}
	if err := s.haltedErr(); err != nil {
		return err
	}
	if !s.Drain(deadline) {
		return errors.New("forward backlog not empty")
	}
	s.guardMu.Lock()
	s.mu.Lock()
	s.src, s.dst = s.dst, s.src
	s.dirEpoch++
	s.mu.Unlock()
	s.srcRun = ""
	s.guardMu.Unlock()
	s.beatSent.Store(0) // one in flight on the old src never comes back
	s.beatLag.Store(0)
	// Writes are paused and both sides were just verified: tracking restarts from equal.
	s.startTracking()
	if s.comparedResync.Load() >= 0 {
		s.comparedResync.Store(s.resyncs.Load())
	}
	s.closeSub()
	return s.subscribe(time.Until(deadline))
}

func (s *Syncer) Stop() {
	s.mu.Lock()
	s.stopped = true
	s.mu.Unlock()
	s.closeSub()
	for _, sh := range s.shards {
		sh.cond.Broadcast()
	}
	s.wg.Wait()
}

func (s *Syncer) Status() map[string]any {
	src, dst, _ := s.dir()
	n, age := s.Backlog()
	mode := "dump"
	if s.logical.Load() {
		mode = "logical"
	}
	return map[string]any{
		"src": src.Name, "dst": dst.Name, "include": s.include, "subscribed": s.subscribed.Load(),
		"backlog": n, "oldest_dirty_age_ms": age, "copy_mode": mode,
		"copied": s.copied.Load(), "deleted": s.deleted.Load(), "errors": s.errs.Load(),
		"marked": s.marked.Load(), "notifications": s.notifications.Load(), "resyncs": s.resyncs.Load(),
		"last_full_scan_ms": s.lastFullScan.Load(), "last_error": s.lastErr.Load(),
		"compare_valid": s.compareValid(), "comparing": s.comparing.Load(), "delta_keys": s.deltaSize(),
		"last_compare": s.lastCompare.Load(), "halted": s.haltReason(),
		"lag_ms": s.lagMs(), "stable_ms": s.stableMs(), "copy_rate": s.copyRate.Load(),
		"compare_rate": s.compareRate.Load(), "est_verify_ms": s.estVerifyMs(),
	}
}

func (s *Syncer) worker(sh *shard) {
	defer s.wg.Done()
	cp := &copier{s: s}
	defer cp.close()
	for {
		sh.mu.Lock()
		for (len(sh.dirty) == 0 || s.haltReason() != "") && !s.isStopped() {
			sh.cond.Wait()
		}
		if s.isStopped() {
			sh.mu.Unlock()
			return
		}
		batch := map[string]int64{}
		keys := make([]string, 0, copyBatch)
		for k, t := range sh.dirty {
			batch[k] = t
			keys = append(keys, k)
			delete(sh.dirty, k) // before reading src: a write after this point marks it again
			if len(keys) == copyBatch {
				break
			}
		}
		sh.copying += len(keys)
		sh.mu.Unlock()

		copied, deleted, err := 0, 0, error(nil)
		for db, group := range groupByDB(keys) {
			raw := make([][]byte, len(group))
			for i, k := range group {
				raw[i] = []byte(k)
			}
			c, d, e := cp.copyMany(db, raw)
			copied, deleted = copied+c, deleted+d
			if e != nil {
				err = e
				break
			}
		}

		sh.mu.Lock()
		sh.copying -= len(keys)
		if err != nil {
			for k, since := range batch {
				if _, again := sh.dirty[k]; !again {
					sh.dirty[k] = since // retry, keep the original age
				}
			}
		}
		sh.mu.Unlock()
		if err != nil {
			s.errs.Add(1)
			s.lastErr.Store(err.Error())
			cp.reset()
			time.Sleep(50 * time.Millisecond)
			continue
		}
		s.copied.Add(int64(copied))
		s.deleted.Add(int64(deleted))
	}
}

// ---- precompare ------------------------------------------------------------------

func (s *Syncer) startTracking() {
	s.deltaMu.Lock()
	s.tracking, s.delta, s.rolling = true, map[string]struct{}{}, nil
	s.rollGen++
	s.deltaMu.Unlock()
}

func (s *Syncer) deltaSize() int {
	s.deltaMu.Lock()
	defer s.deltaMu.Unlock()
	return len(s.delta) + len(s.rolling)
}

// deltaKeys is what has not been compared since it last changed, a compare in flight included.
func (s *Syncer) deltaKeys() []string {
	s.deltaMu.Lock()
	defer s.deltaMu.Unlock()
	out := make([]string, 0, len(s.delta)+len(s.rolling))
	for k := range s.delta {
		out = append(out, k)
	}
	for k := range s.rolling {
		if _, again := s.delta[k]; !again {
			out = append(out, k)
		}
	}
	return out
}

// roll compares the remembered keys outside the pause. A key that changes after the swap
// is marked again, so one that compares equal here can leave the set: same reasoning as
// the precompare, for a smaller set.
func (s *Syncer) roll() {
	defer s.wg.Done()
	defer s.rollBusy.Store(false)
	s.deltaMu.Lock()
	if !s.tracking || len(s.delta) == 0 {
		s.deltaMu.Unlock()
		return
	}
	gen := s.rollGen
	batch := s.delta
	s.rolling, s.delta = batch, map[string]struct{}{}
	s.deltaMu.Unlock()

	t0 := time.Now()
	cmp := s.newComparer(t0.Add(rollTimeout))
	group := make([]string, 0, verifyBatch)
	for k := range batch {
		group = append(group, k)
		if len(group) == verifyBatch {
			cmp.Submit(group)
			group = make([]string, 0, verifyBatch)
		}
	}
	if len(group) > 0 {
		cmp.Submit(group)
	}
	bad, n, err := cmp.Wait()

	s.deltaMu.Lock()
	if s.rollGen == gen {
		s.rolling = nil
		if err != nil { // not compared: they stay remembered
			for k := range batch {
				s.delta[k] = struct{}{}
			}
		}
	}
	s.deltaMu.Unlock()
	if err != nil {
		s.lastErr.Store("rolling compare: " + err.Error())
		return
	}
	s.storeCompareRate(n, time.Since(t0))
	for _, k := range bad {
		s.Mark(k)
	}
}

func (s *Syncer) storeCompareRate(n int, d time.Duration) {
	if ms := d.Milliseconds(); n >= verifyBatch && ms > 0 {
		s.compareRate.Store(int64(n) * 1000 / ms)
	}
}

// estVerifyMs is how long the pause would spend comparing what is remembered now.
func (s *Syncer) estVerifyMs() int64 {
	if rate := s.compareRate.Load(); rate > 0 {
		return int64(s.deltaSize()) * 1000 / rate
	}
	return 0
}

// ---- lag -------------------------------------------------------------------------

func (s *Syncer) beatArrived() {
	if sent := s.beatSent.Swap(0); sent > 0 {
		s.beatLag.Store(time.Now().UnixMilli() - sent)
	}
}

func (s *Syncer) lagMs() int64 {
	lag := s.beatLag.Load()
	if sent := s.beatSent.Load(); sent > 0 {
		lag = max(lag, time.Now().UnixMilli()-sent)
	}
	return lag
}

func (s *Syncer) stableMs() int64 {
	if since := s.stableSince.Load(); since > 0 {
		return time.Now().UnixMilli() - since
	}
	return 0
}

func (s *Syncer) kept() bool {
	n, _ := s.Backlog()
	drainMs := int64(n) * 1000 / max(s.copyRate.Load(), 1000)
	return s.subscribed.Load() && !s.scanning.Load() && s.haltReason() == "" && s.lagMs() <= healthyLagMs &&
		drainMs <= healthyLagMs
}

// monitor sends heartbeats, keeps the rates and starts the rolling compare.
func (s *Syncer) monitor() {
	defer s.wg.Done()
	var c *redisx.Conn
	var epoch uint64
	defer func() {
		if c != nil {
			c.Close()
		}
	}()
	lastDone, lastRate, lastRoll := int64(0), time.Now(), time.Time{}
	for !s.isStopped() {
		time.Sleep(monitorEvery)
		now := time.Now()
		if d := now.Sub(lastRate); d >= time.Second {
			done := s.copied.Load() + s.deleted.Load()
			s.copyRate.Store((done - lastDone) * 1000 / d.Milliseconds())
			lastDone, lastRate = done, now
		}
		c, epoch = s.beat(c, epoch)
		if s.kept() {
			s.stableSince.CompareAndSwap(0, now.UnixMilli())
		} else {
			s.stableSince.Store(0)
		}
		if s.compareValid() && !s.comparing.Load() && now.Sub(lastRoll) >= rollEvery && s.rollBusy.CompareAndSwap(false, true) {
			lastRoll = now
			s.wg.Add(1)
			go s.roll()
		}
	}
}

// beat writes a heartbeat if none is in flight. A frozen src must not freeze this loop.
func (s *Syncer) beat(c *redisx.Conn, epoch uint64) (*redisx.Conn, uint64) {
	src, _, cur := s.dir()
	if c != nil && cur != epoch {
		c.Close()
		c = nil
	}
	if c == nil {
		var err error
		if c, err = src.Dial(0); err != nil {
			return nil, cur
		}
	}
	now := time.Now().UnixMilli()
	if sent := s.beatSent.Load(); sent > 0 && now-sent < lostBeatMs {
		return c, cur
	}
	c.NetConn().SetDeadline(time.Now().Add(2 * time.Second))
	s.beatSent.Store(now)
	if _, err := c.DoStr("SET", heartbeatKey, strconv.FormatInt(now, 10), "PX", "10000"); err != nil {
		c.Close()
		return nil, cur
	}
	return c, cur
}

func (s *Syncer) compareValid() bool {
	base := s.comparedResync.Load()
	return base >= 0 && base == s.resyncs.Load() && s.haltReason() == ""
}

// Precompare compares both sides while writes continue. A key that differs is
// marked and copied again; a key changing during the pass is marked by its
// notification. Valid until the subscription drops.
func (s *Syncer) Precompare(deadline time.Time) (map[string]any, error) {
	if err := s.haltedErr(); err != nil {
		return nil, err
	}
	if !s.comparing.CompareAndSwap(false, true) {
		return nil, errors.New("precompare already running")
	}
	defer s.comparing.Store(false)
	t0 := time.Now()
	base := s.resyncs.Load()
	s.comparedResync.Store(-1)
	s.startTracking()
	// Not until it is empty: under steady writes it never is. Only so the pass does not
	// compare what is still being copied for the first time.
	for {
		n, _ := s.Backlog()
		if n <= settledBacklog && !s.scanning.Load() {
			break
		}
		if time.Now().After(deadline) || s.isStopped() {
			return nil, errors.New("precompare: backlog did not settle in time")
		}
		time.Sleep(50 * time.Millisecond)
	}
	srcEP, dstEP, _ := s.dir()
	src, err := srcEP.Dial(0)
	if err != nil {
		return nil, err
	}
	defer src.Close()
	dst, err := dstEP.Dial(0)
	if err != nil {
		return nil, err
	}
	defer dst.Close()

	// A key that differs is marked, so it stays remembered until it is copied and compared:
	// nothing needs to be level when this pass ends.
	differed := 0
	dbs, err := dbsOf(src, dst)
	if err != nil {
		return nil, err
	}
	cmp := s.newComparer(deadline)
	err = s.scanDBs(src, dbs, func(db int, keys []string) error {
		for i := 0; i < len(keys); i += verifyBatch {
			group := make([]string, 0, verifyBatch)
			for _, k := range keys[i:min(i+verifyBatch, len(keys))] {
				group = append(group, dkey(db, k))
			}
			cmp.Submit(group)
		}
		return nil
	})
	if err == nil {
		err = s.scanDBs(dst, dbs, func(db int, keys []string) error { // keys only on dst
			only, err := missingOn(src, db, keys)
			differed += len(only)
			for _, k := range only {
				s.Mark(dkey(db, k))
			}
			return err
		})
	}
	if err != nil {
		cmp.Wait()
		return nil, err
	}
	bad, compared, err := cmp.Wait()
	if err != nil {
		return nil, err
	}
	differed += len(bad)
	for _, k := range bad {
		s.Mark(k)
	}
	s.storeCompareRate(compared, time.Since(t0))
	res := map[string]any{"compared": compared, "differed": differed, "ms": float64(time.Since(t0).Microseconds()) / 1000}
	s.lastCompare.Store(res)
	if s.resyncs.Load() != base {
		return res, errors.New("precompare: subscription dropped during the pass")
	}
	s.comparedResync.Store(base)
	return res, nil
}

func (s *Syncer) barrierArrived() {
	s.barrierMu.Lock()
	if s.barrierSeen != nil {
		close(s.barrierSeen)
		s.barrierSeen = nil
	}
	s.barrierMu.Unlock()
}

// ReconcileDelta is the final reconcile of delta mode. Writes are paused: once the
// barrier's notification arrives, every earlier change's notification has too.
func (s *Syncer) ReconcileDelta(deadline time.Time) (int, error) {
	if err := s.haltedErr(); err != nil {
		return 0, err
	}
	if !s.compareValid() {
		return 0, errors.New("no valid precompare: the subscription dropped or the compare has not finished")
	}
	seen := make(chan struct{})
	s.barrierMu.Lock()
	s.barrierSeen = seen
	s.barrierMu.Unlock()
	srcEP, _, _ := s.dir()
	src, err := srcEP.Dial(0)
	if err != nil {
		return 0, err
	}
	defer src.Close()
	if _, err := src.DoStr("SET", barrierKey, strconv.FormatInt(time.Now().UnixNano(), 10), "PX", "10000"); err != nil {
		return 0, err
	}
	select {
	case <-seen:
	case <-time.After(time.Until(deadline)):
		return 0, errors.New("barrier notification did not arrive in time")
	}
	if !s.compareValid() {
		return 0, errors.New("the subscription dropped during the pause")
	}
	if !s.Drain(deadline) {
		n, _ := s.Backlog()
		return 0, errors.New("drain timeout (backlog " + strconv.Itoa(n) + ")")
	}
	return s.deltaSize(), nil
}

func (s *Syncer) isStopped() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stopped
}

// ---- copier ----------------------------------------------------------------------

type copier struct {
	s        *Syncer
	src, dst *redisx.Conn
	epoch    uint64
}

func (c *copier) conns() (*redisx.Conn, *redisx.Conn, error) {
	srcEP, dstEP, epoch := c.s.dir()
	if epoch != c.epoch {
		c.reset()
		c.epoch = epoch
	}
	var err error
	if c.src == nil {
		conn, err := srcEP.Dial(0)
		if err != nil {
			return nil, nil, err
		}
		if err := c.s.checkSource(conn, srcEP); err != nil {
			conn.Close()
			return nil, nil, err
		}
		c.src = conn
	}
	if c.dst == nil {
		if c.dst, err = dstEP.Dial(0); err != nil {
			return nil, nil, err
		}
	}
	return c.src, c.dst, nil
}

func (c *copier) reset() {
	if c.src != nil {
		c.src.Close()
		c.src = nil
	}
	if c.dst != nil {
		c.dst.Close()
		c.dst = nil
	}
}

func (c *copier) close() { c.reset() }

// copyMany copies keys of one db in as few round trips as it can.
func (c *copier) copyMany(db int, keys [][]byte) (copied, deleted int, err error) {
	src, dst, err := c.conns()
	if err != nil {
		return 0, 0, err
	}
	if err := src.Select(db); err != nil {
		return 0, 0, err
	}
	if err := dst.Select(db); err != nil {
		return 0, 0, err
	}
	if !c.s.logical.Load() {
		copied, deleted, err = copyDumpMany(src, dst, keys)
		if err == nil || !isPayloadErr(err) {
			return copied, deleted, err
		}
		log.Printf("dst rejects DUMP payload (%v) → logical copy", err)
		c.s.logical.Store(true)
	}
	for _, k := range keys {
		res, err := copyLogical(src, dst, k)
		if err != nil {
			return copied, deleted, err
		}
		if res == "deleted" {
			deleted++
		} else {
			copied++
		}
	}
	return copied, deleted, nil
}

func isPayloadErr(err error) bool {
	m := strings.ToLower(err.Error())
	return strings.Contains(m, "payload version") || strings.Contains(m, "bad data format") ||
		strings.Contains(m, "unknown command")
}

var b = redisx.B

// copyDumpMany: DUMP+PTTL atomically on the source, RESTORE ... REPLACE on the target.
func copyDumpMany(src, dst *redisx.Conn, keys [][]byte) (copied, deleted int, err error) {
	read := make([][][]byte, 0, len(keys)*2)
	for _, k := range keys {
		read = append(read, [][]byte{b("DUMP"), k}, [][]byte{b("PTTL"), k})
	}
	r, err := src.Tx(read...)
	if err != nil {
		return 0, 0, err
	}
	write := make([][][]byte, len(keys))
	for i, k := range keys {
		dump, ttl := r[i*2], r[i*2+1].Int
		if dump.Null {
			write[i] = [][]byte{b("DEL"), k}
			deleted++
			continue
		}
		if ttl < 0 {
			ttl = 0
		}
		write[i] = [][]byte{b("RESTORE"), k, b(strconv.FormatInt(ttl, 10)), dump.Str, b("REPLACE")}
		copied++
	}
	replies, err := dst.Pipeline(write...)
	if err != nil {
		return 0, 0, err
	}
	for _, v := range replies {
		if v.IsErr() {
			return 0, 0, v.Err()
		}
	}
	return copied, deleted, nil
}

var errUnsupportedType = errors.New("unsupported type")

// copyLogical: per-type read on the source (in MULTI, with TYPE re-checked) and
// DEL + rebuild + PEXPIRE in one MULTI on the target.
func copyLogical(src, dst *redisx.Conn, key []byte) (string, error) {
	t, err := src.Do(b("TYPE"), key)
	if err != nil {
		return "", err
	}
	typ := string(t.Str)
	if typ == "none" {
		_, err := dst.Do(b("DEL"), key)
		return "deleted", err
	}
	read := readCmd(typ, key)
	if read == nil {
		return "", errUnsupportedType
	}
	r, err := src.Tx([][]byte{b("TYPE"), key}, read, [][]byte{b("PTTL"), key})
	if err != nil {
		return "", err
	}
	if string(r[0].Str) != typ {
		return "", errors.New("type changed during copy") // retried
	}
	val, pttl := r[1], r[2].Int

	cmds := [][][]byte{{b("DEL"), key}}
	flat := func(cmd string, vs []redisx.Value) [][]byte {
		out := [][]byte{b(cmd), key}
		for _, v := range vs {
			out = append(out, v.Str)
		}
		return out
	}
	switch typ {
	case "string":
		if !val.Null {
			cmds = append(cmds, [][]byte{b("SET"), key, val.Str})
		}
	case "hash":
		if len(val.Arr) > 0 {
			cmds = append(cmds, flat("HSET", val.Arr))
		}
	case "list":
		if len(val.Arr) > 0 {
			cmds = append(cmds, flat("RPUSH", val.Arr))
		}
	case "set":
		if len(val.Arr) > 0 {
			cmds = append(cmds, flat("SADD", val.Arr))
		}
	case "zset":
		if len(val.Arr) > 0 {
			z := [][]byte{b("ZADD"), key}
			for i := 0; i+1 < len(val.Arr); i += 2 {
				z = append(z, val.Arr[i+1].Str, val.Arr[i].Str) // score member
			}
			cmds = append(cmds, z)
		}
	case "stream":
		for _, e := range val.Arr {
			x := [][]byte{b("XADD"), key, e.Arr[0].Str}
			for _, f := range e.Arr[1].Arr {
				x = append(x, f.Str)
			}
			cmds = append(cmds, x)
		}
	}
	if pttl > 0 {
		cmds = append(cmds, [][]byte{b("PEXPIRE"), key, b(strconv.FormatInt(pttl, 10))})
	}
	_, err = dst.Tx(cmds...)
	return "copied", err
}

func readCmd(typ string, key []byte) [][]byte {
	return map[string][][]byte{
		"string": {b("GET"), key},
		"hash":   {b("HGETALL"), key},
		"list":   {b("LRANGE"), key, b("0"), b("-1")},
		"set":    {b("SMEMBERS"), key},
		"zset":   {b("ZRANGE"), key, b("0"), b("-1"), b("WITHSCORES")},
		"stream": {b("XRANGE"), key, b("-"), b("+")},
	}[typ]
}
