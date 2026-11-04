package main

// Needs two real servers, which may be different engines:
//   BUCKET_TEST_ENDPOINTS="a=redis://127.0.0.1:16611,b=redis://127.0.0.1:16612" go test ./cmd/syncworker/
// Both are flushed.

import (
	"fmt"
	"os"
	"testing"
	"time"

	"statemig/internal/redisx"
)

func bucketPair(t *testing.T) (*Syncer, *redisx.Conn, *redisx.Conn) {
	t.Helper()
	spec := os.Getenv("BUCKET_TEST_ENDPOINTS")
	if spec == "" {
		t.Skip("BUCKET_TEST_ENDPOINTS not set")
	}
	eps, order, err := redisx.ParseEndpoints(spec)
	if err != nil || len(order) != 2 {
		t.Fatalf("endpoints: %v", err)
	}
	s := NewSyncer(eps[order[0]], eps[order[1]], []string{"session:", "cache:", "q:"}, 1)
	var conns []*redisx.Conn
	for _, name := range order {
		c, err := eps[name].Dial(0)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { c.Close() })
		if _, err := c.DoStr("FLUSHALL"); err != nil {
			t.Fatal(err)
		}
		conns = append(conns, c)
	}
	return s, conns[0], conns[1]
}

// seed writes the same data on c, in an order that depends on rev, so encodings and member
// order differ between the two sides while the values are equal.
func seed(t *testing.T, c *redisx.Conn, rev bool) {
	t.Helper()
	do := func(args ...string) {
		if _, err := c.DoStr(args...); err != nil {
			t.Fatalf("%v: %v", args, err)
		}
	}
	idx := func(i, n int) int {
		if rev {
			return n - 1 - i
		}
		return i
	}
	for i := 0; i < 3000; i++ {
		j := idx(i, 3000)
		do("SET", fmt.Sprintf("session:%d", j), fmt.Sprintf("v%d", j))
	}
	for i := 0; i < 50; i++ {
		j := idx(i, 50)
		do("SET", fmt.Sprintf("session:ttl:%d", j), "x", "EX", "3600")
		do("HSET", "session:h", fmt.Sprintf("f%d", j), fmt.Sprintf("%d", j))
		do("SADD", "session:s", fmt.Sprintf("m%d", j))
		do("ZADD", "session:z", fmt.Sprintf("%d.1", j), fmt.Sprintf("m%d", j))
	}
	do("RPUSH", "session:l", "a", "b", "c")
	do("XADD", "q:stream", "1-1", "a", "1")
	do("SET", "other:ignored", fmt.Sprintf("%v", rev)) // not included: may differ
}

func TestBucketCompareEqualAcrossEngines(t *testing.T) {
	s, a, b := bucketPair(t)
	seed(t, a, false)
	seed(t, b, true)
	res, err := s.bucketCompare(time.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if res["buckets_differed"].(int) != 0 || res["differed"].(int) != 0 {
		t.Fatalf("equal data, but %v", res)
	}
}

func TestBucketCompareFindsEachDifference(t *testing.T) {
	s, a, b := bucketPair(t)
	seed(t, a, false)
	seed(t, b, false)
	for _, cmd := range [][]string{
		{"SET", "session:7", "changed"},        // value
		{"DEL", "session:8"},                   // missing on dst
		{"SET", "session:only-dst", "x"},       // only on dst
		{"HSET", "session:h", "f3", "changed"}, // inside a hash
		{"EXPIRE", "session:ttl:4", "60"},      // expiry, an hour apart
		{"PERSIST", "session:ttl:5"},           // expiry removed
		{"ZADD", "session:z", "9.25", "m9"},    // a score
		{"XADD", "q:stream", "2-1", "b", "2"},  // a stream entry
	} {
		if _, err := b.DoStr(cmd...); err != nil {
			t.Fatal(err)
		}
	}
	res, err := s.bucketCompare(time.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"session:7", "session:8", "session:only-dst", "session:h", "session:ttl:4",
		"session:ttl:5", "session:z", "q:stream"}
	got := map[string]bool{}
	for _, k := range s.deltaKeysAll() {
		got[k] = true
	}
	for _, k := range want {
		if !got[dkey(0, k)] {
			t.Errorf("%s not marked; %v", k, res)
		}
	}
	if len(got) != len(want) {
		t.Errorf("marked %d keys, want %d: %v", len(got), len(want), got)
	}
	if res["keys_listed"].(int) > 64*len(want)*4 {
		t.Errorf("listed %v keys for %d differences", res["keys_listed"], len(want))
	}
	t.Logf("%v", res)
}

// deltaKeysAll is every marked key still waiting to be copied (no worker runs in the test).
func (s *Syncer) deltaKeysAll() []string {
	var out []string
	for _, sh := range s.shards {
		sh.mu.Lock()
		for k := range sh.dirty {
			out = append(out, k)
		}
		sh.mu.Unlock()
	}
	return out
}
