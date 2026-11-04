package main

// Bucket compare: the precompare, and the rescan after a dropped subscription, without moving
// the values. Each side hashes its own keys inside Redis (Lua) into buckets by key name; only
// the bucket hashes cross the network. Where a bucket differs, both sides list that bucket's
// keys with a digest each, and only the keys that differ are marked and copied.
//
// A key's digest covers its name, type and value (hash fields and set members sorted, zset
// scores formatted the same way on both sides) and, in the bucket hash, its expiry time
// rounded to a minute. Rounding makes a key whose two expiry times straddle a minute boundary
// look different: its bucket is then listed and the key compared exactly (2s tolerance, as the
// verify), so this costs a listing, not a copy. An expiry that differs by less than a minute
// can go unnoticed when both fall in the same minute.

import (
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"statemig/internal/redisx"
)

const (
	sumScanCount   = 1000 // keys per hashing call: about 10ms of Redis time
	listScanCount  = 5000 // keys per listing call: most are only checked for their bucket
	minBuckets     = 1024
	maxBuckets     = 1 << 18
	keysPerBucket  = 16   // a differing bucket costs listing about this many keys
	expiryTolMs    = 2000 // as the verify
	bucketModeSum  = "sum"
	bucketModeList = "list"
)

var errNoBuckets = errors.New("bucket compare not available")

const bucketScript = `-- bucket compare v1
local cursor, count, n, mode, bitmap = ARGV[1], ARGV[2], tonumber(ARGV[3]), ARGV[4], ARGV[5]
local prefixes = {}
for i = 6, #ARGV do prefixes[#prefixes + 1] = ARGV[i] end
local r = redis.call('SCAN', cursor, 'COUNT', count)
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local function included(k)
  for _, p in ipairs(prefixes) do
    if string.sub(k, 1, #p) == p then return true end
  end
  return false
end
local function sorted(a) table.sort(a) return table.concat(a, '\1') end
local function value(k, typ)
  if typ == 'string' then return redis.call('GET', k) end
  if typ == 'hash' then
    local a, p = redis.call('HGETALL', k), {}
    for i = 1, #a, 2 do p[#p + 1] = a[i] .. '\0' .. a[i + 1] end
    return sorted(p)
  end
  if typ == 'set' then return sorted(redis.call('SMEMBERS', k)) end
  if typ == 'list' then return table.concat(redis.call('LRANGE', k, 0, -1), '\1') end
  if typ == 'zset' then
    local a, p = redis.call('ZRANGE', k, 0, -1, 'WITHSCORES'), {}
    for i = 1, #a, 2 do p[#p + 1] = a[i] .. '\0' .. string.format('%.17g', tonumber(a[i + 1])) end
    return table.concat(p, '\1')
  end
  if typ == 'stream' then
    local a, p = redis.call('XRANGE', k, '-', '+'), {}
    for _, e in ipairs(a) do p[#p + 1] = e[1] .. '\0' .. table.concat(e[2], '\0') end
    return table.concat(p, '\1')
  end
  return nil
end
local out, acc, order = {}, {}, {}
for _, k in ipairs(r[2]) do
  if included(k) then
    local b = tonumber(string.sub(redis.sha1hex(k), 1, 8), 16) % n
    local want = true
    if mode == 'list' then
      local byte = string.byte(bitmap, math.floor(b / 8) + 1)
      want = byte ~= nil and bit.band(byte, bit.lshift(1, b % 8)) ~= 0
    end
    if want then
      local typ = redis.call('TYPE', k)['ok']
      if typ ~= 'none' then
        local v = value(k, typ)
        if v == nil then return redis.error_reply('ERR unsupported type ' .. typ .. ' at ' .. k) end
        local ttl = redis.call('PTTL', k)
        local exp = -1
        if ttl >= 0 then exp = now + ttl end
        local d = redis.sha1hex(k .. '\0' .. typ .. '\0' .. v)
        if mode == 'list' then
          out[#out + 1] = k
          out[#out + 1] = string.sub(d, 1, 16)
          out[#out + 1] = exp
        else
          local w = -1
          if exp >= 0 then w = math.floor(exp / ` + "60000" + `) end
          local e = redis.sha1hex(d .. ':' .. w)
          local hi, lo = tonumber(string.sub(e, 1, 8), 16), tonumber(string.sub(e, 9, 16), 16)
          local a = acc[b]
          if a == nil then
            acc[b] = {hi, lo}
            order[#order + 1] = b
          else
            a[1], a[2] = bit.bxor(a[1], hi), bit.bxor(a[2], lo)
          end
        end
      end
    end
  end
end
for _, b in ipairs(order) do
  out[#out + 1] = b
  out[#out + 1] = bit.tobit(acc[b][1])
  out[#out + 1] = bit.tobit(acc[b][2])
end
return {r[1], out}
`

// bucketRunner calls the script on one connection, loading it when the server lost it.
type bucketRunner struct {
	c   *redisx.Conn
	sha string
}

func newBucketRunner(c *redisx.Conn) (*bucketRunner, error) {
	br := &bucketRunner{c: c}
	return br, br.load()
}

func (br *bucketRunner) load() error {
	v, err := br.c.DoStr("SCRIPT", "LOAD", bucketScript)
	if err != nil {
		return fmt.Errorf("%w: %v", errNoBuckets, err)
	}
	br.sha = string(v.Str)
	return nil
}

// call runs one SCAN step: the next cursor and the flat result.
func (br *bucketRunner) call(cursor string, n int, mode string, bitmap []byte, prefixes []string) (string, []redisx.Value, error) {
	count := sumScanCount
	if mode == bucketModeList {
		count = listScanCount
	}
	args := []string{"EVALSHA", br.sha, "0", cursor, strconv.Itoa(count), strconv.Itoa(n), mode, string(bitmap)}
	args = append(args, prefixes...)
	v, err := br.c.DoStr(args...)
	if err != nil && strings.HasPrefix(err.Error(), "NOSCRIPT") {
		if err = br.load(); err == nil {
			args[1] = br.sha
			v, err = br.c.DoStr(args...)
		}
	}
	if err != nil {
		if strings.Contains(err.Error(), "unsupported type") {
			return "", nil, err
		}
		return "", nil, fmt.Errorf("%w: %v", errNoBuckets, err)
	}
	if len(v.Arr) != 2 {
		return "", nil, fmt.Errorf("%w: unexpected reply", errNoBuckets)
	}
	return string(v.Arr[0].Str), v.Arr[1].Arr, nil
}

// bucketCount scales with the keys, so a differing bucket stays about keysPerBucket keys.
func bucketCount(keys int) int {
	n := minBuckets
	for n < maxBuckets && n*keysPerBucket < keys {
		n *= 2
	}
	return n
}

// bucketSums hashes every included key of ep, db by db.
func (s *Syncer) bucketSums(ep *redisx.Endpoint, dbs []int, n int, deadline time.Time) (map[int][]uint64, error) {
	c, err := ep.Dial(0)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	br, err := newBucketRunner(c)
	if err != nil {
		return nil, err
	}
	out := map[int][]uint64{}
	for _, db := range dbs {
		if err := c.Select(db); err != nil {
			return nil, err
		}
		sums := make([]uint64, n)
		cursor := "0"
		for {
			if err := s.stepAllowed(deadline); err != nil {
				return nil, err
			}
			next, flat, err := br.call(cursor, n, bucketModeSum, nil, s.include)
			if err != nil {
				return nil, err
			}
			for i := 0; i+2 < len(flat); i += 3 {
				b := flat[i].Int
				sums[b] ^= uint64(uint32(flat[i+1].Int))<<32 | uint64(uint32(flat[i+2].Int))
			}
			if cursor = next; cursor == "0" {
				break
			}
		}
		out[db] = sums
	}
	return out, nil
}

type keyDigest struct {
	digest string
	exp    int64 // unix ms, -1 = no expiry
}

// bucketKeys lists ep's keys in the buckets set in bitmaps, with their digests.
func (s *Syncer) bucketKeys(ep *redisx.Endpoint, bitmaps map[int][]byte, n int, deadline time.Time) (map[string]keyDigest, error) {
	c, err := ep.Dial(0)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	br, err := newBucketRunner(c)
	if err != nil {
		return nil, err
	}
	out := map[string]keyDigest{}
	for db, bm := range bitmaps {
		if err := c.Select(db); err != nil {
			return nil, err
		}
		cursor := "0"
		for {
			if err := s.stepAllowed(deadline); err != nil {
				return nil, err
			}
			next, flat, err := br.call(cursor, n, bucketModeList, bm, s.include)
			if err != nil {
				return nil, err
			}
			for i := 0; i+2 < len(flat); i += 3 {
				out[dkey(db, string(flat[i].Str))] = keyDigest{string(flat[i+1].Str), flat[i+2].Int}
			}
			if cursor = next; cursor == "0" {
				break
			}
		}
	}
	return out, nil
}

func (s *Syncer) stepAllowed(deadline time.Time) error {
	if s.isStopped() {
		return errors.New("sync stopped")
	}
	if past(deadline) {
		return errors.New("bucket compare: deadline exceeded")
	}
	return nil
}

func sameDigest(a, d keyDigest) bool {
	if a.digest != d.digest || (a.exp < 0) != (d.exp < 0) {
		return false
	}
	return a.exp < 0 || abs64(a.exp-d.exp) <= expiryTolMs
}

// bucketCompare compares both sides by buckets and marks the keys that differ. Both sides run
// at once; the values never leave Redis.
func (s *Syncer) bucketCompare(deadline time.Time) (map[string]any, error) {
	t0 := time.Now()
	srcEP, dstEP, _ := s.dir()
	var dbs []int
	var keys int
	err := pool.withConn(srcEP, func(src *redisx.Conn) error {
		return pool.withConn(dstEP, func(dst *redisx.Conn) error {
			var err error
			if dbs, err = dbsOf(src, dst); err != nil {
				return err
			}
			for _, c := range []*redisx.Conn{src, dst} {
				n, err := dbKeys(c)
				if err != nil {
					return err
				}
				keys = max(keys, n)
			}
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	n := bucketCount(keys)

	var ss, ds map[int][]uint64
	var serr, derr error
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); ss, serr = s.bucketSums(srcEP, dbs, n, deadline) }()
	go func() { defer wg.Done(); ds, derr = s.bucketSums(dstEP, dbs, n, deadline) }()
	wg.Wait()
	if err := errors.Join(serr, derr); err != nil {
		return nil, err
	}
	t1 := time.Now()

	bitmaps, differing := map[int][]byte{}, 0
	for _, db := range dbs {
		var bm []byte
		for b := 0; b < n; b++ {
			if ss[db][b] != ds[db][b] {
				if bm == nil {
					bm = make([]byte, (n+7)/8)
				}
				bm[b/8] |= 1 << (b % 8)
				differing++
			}
		}
		if bm != nil {
			bitmaps[db] = bm
		}
	}

	marked, listed := 0, 0
	if differing > 0 {
		var sk, dk map[string]keyDigest
		wg.Add(2)
		go func() { defer wg.Done(); sk, serr = s.bucketKeys(srcEP, bitmaps, n, deadline) }()
		go func() { defer wg.Done(); dk, derr = s.bucketKeys(dstEP, bitmaps, n, deadline) }()
		wg.Wait()
		if err := errors.Join(serr, derr); err != nil {
			return nil, err
		}
		listed = max(len(sk), len(dk))
		for k, a := range sk {
			if d, ok := dk[k]; !ok || !sameDigest(a, d) {
				s.Mark(k)
				marked++
			}
		}
		for k := range dk {
			if _, ok := sk[k]; !ok { // only on dst: the copier deletes it
				s.Mark(k)
				marked++
			}
		}
	}
	res := map[string]any{"method": "buckets", "buckets": n, "buckets_differed": differing,
		"keys_listed": listed, "differed": marked, "hash_ms": t1.Sub(t0).Milliseconds(),
		"ms": float64(time.Since(t0).Microseconds()) / 1000}
	log.Printf("bucket compare: %d buckets, %d differed, %d keys listed, %d marked, %v", n, differing, listed,
		marked, time.Since(t0).Round(time.Millisecond))
	return res, nil
}

// dbKeys sums the keys of every db (INFO keyspace), to size the buckets.
func dbKeys(c *redisx.Conn) (int, error) {
	v, err := c.DoStr("INFO", "keyspace")
	if err != nil {
		return 0, err
	}
	n := 0
	for _, line := range strings.Split(string(v.Str), "\n") {
		if !strings.HasPrefix(line, "db") {
			continue
		}
		if i := strings.Index(line, "keys="); i >= 0 {
			f := line[i+5:]
			if j := strings.IndexByte(f, ','); j >= 0 {
				f = f[:j]
			}
			k, _ := strconv.Atoi(strings.TrimSpace(f))
			n += k
		}
	}
	return n, nil
}
