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
//   - Precompare (delta mode): both sides are compared while writes continue and
//     every key marked from then on is remembered. As long as the subscription
//     never dropped, a key outside that set still equals what was compared, so the
//     cutover only has to finish and check those keys. A barrier key written after
//     the pause proves every earlier notification has arrived.

import (
	"errors"
	"hash/fnv"
	"log"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"statemig/internal/redisx"
)

const (
	notifyPrefix = "__keyspace@0__:"
	barrierKey   = "__statemig:barrier"
	copyBatch    = 100 // keys per round trip, per worker
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
}

func NewSyncer(src, dst *redisx.Endpoint, include []string, workers int) *Syncer {
	s := &Syncer{include: include, src: src, dst: dst}
	s.lastErr.Store("")
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

// Start subscribes to src, waits for the subscription to be confirmed, then
// enqueues a full two-sided scan.
func (s *Syncer) Start(timeout time.Duration) error {
	for _, sh := range s.shards {
		s.wg.Add(1)
		go s.worker(sh)
	}
	if err := s.subscribe(timeout); err != nil {
		return err
	}
	_, err := s.FullScan()
	return err
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
			redisx.WriteCommand(c.Writer(), redisx.B("PSUBSCRIBE"), redisx.B(notifyPrefix+"*"))
			if err = c.Writer().Flush(); err == nil {
				var v redisx.Value
				if v, err = redisx.ReadValue(c.Reader()); err == nil && v.IsErr() { // ["psubscribe", pattern, 1]
					err = v.Err()
				}
			}
		}
		if err != nil {
			s.lastErr.Store("subscribe " + src.Name + ": " + err.Error())
			if c != nil {
				c.Close()
			}
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
				key := strings.TrimPrefix(string(v.Arr[2].Str), notifyPrefix)
				s.notifications.Add(1)
				if key == barrierKey {
					s.barrierArrived()
				} else if s.included(key) {
					s.Mark(key)
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

// FullScan enqueues every included key of both sides.
func (s *Syncer) FullScan() (int, error) {
	src, dst, _ := s.dir()
	n := 0
	for _, ep := range []*redisx.Endpoint{src, dst} {
		c, err := ep.Dial(0)
		if err != nil {
			return n, err
		}
		for _, p := range s.include {
			cursor := "0"
			for {
				v, err := c.DoStr("SCAN", cursor, "MATCH", p+"*", "COUNT", "1000")
				if err != nil {
					c.Close()
					return n, err
				}
				cursor = string(v.Arr[0].Str)
				for _, k := range v.Arr[1].Arr {
					s.Mark(string(k.Str))
					n++
				}
				if cursor == "0" {
					break
				}
			}
		}
		c.Close()
	}
	s.lastFullScan.Store(time.Now().UnixMilli())
	return n, nil
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
	if !s.Drain(deadline) {
		return errors.New("forward backlog not empty")
	}
	s.mu.Lock()
	s.src, s.dst = s.dst, s.src
	s.dirEpoch++
	s.mu.Unlock()
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
		"last_compare": s.lastCompare.Load(),
	}
}

func (s *Syncer) worker(sh *shard) {
	defer s.wg.Done()
	cp := &copier{s: s}
	defer cp.close()
	for {
		sh.mu.Lock()
		for len(sh.dirty) == 0 && !s.isStopped() {
			sh.cond.Wait()
		}
		if s.isStopped() {
			sh.mu.Unlock()
			return
		}
		batch := map[string]int64{}
		keys := make([][]byte, 0, copyBatch)
		for k, t := range sh.dirty {
			batch[k] = t
			keys = append(keys, []byte(k))
			delete(sh.dirty, k) // before reading src: a write after this point marks it again
			if len(keys) == copyBatch {
				break
			}
		}
		sh.copying += len(keys)
		sh.mu.Unlock()

		copied, deleted, err := cp.copyMany(keys)

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
	s.tracking, s.delta = true, map[string]struct{}{}
	s.deltaMu.Unlock()
}

func (s *Syncer) deltaSize() int {
	s.deltaMu.Lock()
	defer s.deltaMu.Unlock()
	return len(s.delta)
}

func (s *Syncer) deltaKeys() []string {
	s.deltaMu.Lock()
	defer s.deltaMu.Unlock()
	out := make([]string, 0, len(s.delta))
	for k := range s.delta {
		out = append(out, k)
	}
	return out
}

func (s *Syncer) compareValid() bool {
	base := s.comparedResync.Load()
	return base >= 0 && base == s.resyncs.Load()
}

// Precompare compares both sides while writes continue. A key that differs is
// marked and copied again; a key changing during the pass is marked by its
// notification. Valid until the subscription drops.
func (s *Syncer) Precompare(deadline time.Time) (map[string]any, error) {
	if !s.comparing.CompareAndSwap(false, true) {
		return nil, errors.New("precompare already running")
	}
	defer s.comparing.Store(false)
	t0 := time.Now()
	base := s.resyncs.Load()
	s.comparedResync.Store(-1)
	s.startTracking()
	if !s.Drain(deadline) {
		return nil, errors.New("precompare: backlog not drained in time")
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

	compared, differed := 0, 0
	for _, p := range s.include {
		err := scanBatches(src, p, verifyBatch, func(keys []string) error {
			bad, err := s.compareKeys(src, dst, keys)
			if err != nil {
				return err
			}
			compared += len(keys)
			differed += len(bad)
			for _, k := range bad {
				s.Mark(k)
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		err = scanBatches(dst, p, verifyBatch, func(keys []string) error { // keys only on dst
			cmds := make([][][]byte, len(keys))
			for i, k := range keys {
				cmds[i] = [][]byte{b("EXISTS"), []byte(k)}
			}
			r, err := src.Pipeline(cmds...)
			if err != nil {
				return err
			}
			for i, v := range r {
				if v.Int == 0 {
					differed++
					s.Mark(keys[i])
				}
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	if !s.Drain(deadline) {
		return nil, errors.New("precompare: differences not copied in time")
	}
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
		if c.src, err = srcEP.Dial(0); err != nil {
			return nil, nil, err
		}
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

// copyMany copies keys in as few round trips as it can.
func (c *copier) copyMany(keys [][]byte) (copied, deleted int, err error) {
	src, dst, err := c.conns()
	if err != nil {
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
