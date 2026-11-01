package main

// VERIFY: compare src and dst while writes are paused.
//   delta mode:      only the keys marked since the precompare, then key counts and a sample
//   full prefixes:   key sets of both sides must match, then every value and TTL
//   sample prefixes: key counts must agree (TTL expiry may differ a little),
//                    then up to N keys taken from src are compared

import (
	"bytes"
	"fmt"
	"sort"
	"strconv"

	"statemig/internal/redisx"
)

const verifyBatch = 200 // keys per round trip

type VerifyReq struct {
	Full   []string       `json:"full"`
	Sample map[string]int `json:"sample"`
	Mode   string         `json:"mode"` // "" or "delta"
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

// snapshotOf reads a key's type, value (normalised) and TTL.
func snapshotOf(c *redisx.Conn, key string) (string, []byte, int64, error) {
	k := []byte(key)
	t, err := c.Do(b("TYPE"), k)
	if err != nil {
		return "", nil, 0, err
	}
	typ := string(t.Str)
	if typ == "none" {
		return typ, nil, -2, nil
	}
	read := readCmd(typ, k)
	if read == nil {
		return typ, nil, 0, errUnsupportedType
	}
	v, err := c.Do(read...)
	if err != nil {
		return typ, nil, 0, err
	}
	ttl, err := c.Do(b("PTTL"), k)
	if err != nil {
		return typ, nil, 0, err
	}
	return typ, normalise(typ, v), ttl.Int, nil
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

func sameKey(a, d *redisx.Conn, key string) (bool, error) {
	ta, va, la, err := snapshotOf(a, key)
	if err != nil {
		return false, err
	}
	td, vd, ld, err := snapshotOf(d, key)
	if err != nil {
		return false, err
	}
	if ta != td || !bytes.Equal(va, vd) {
		return false, nil
	}
	// TTL: both persistent, or both expiring within 2s of each other
	if (la < 0) != (ld < 0) {
		return false, nil
	}
	if la >= 0 && (la-ld > 2000 || ld-la > 2000) {
		return false, nil
	}
	return true, nil
}

// compareKeys returns the keys that differ, a batch per round trip. Equal DUMP
// bytes settle it; anything else gets the per-type comparison, since the same
// value can have a different encoding.
func (s *Syncer) compareKeys(src, dst *redisx.Conn, keys []string) ([]string, error) {
	var bad []string
	if s.logical.Load() {
		for _, k := range keys {
			same, err := sameKey(src, dst, k)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", k, err)
			}
			if !same {
				bad = append(bad, k)
			}
		}
		return bad, nil
	}
	cmds := make([][][]byte, 0, len(keys)*2)
	for _, k := range keys {
		cmds = append(cmds, [][]byte{b("DUMP"), []byte(k)}, [][]byte{b("PTTL"), []byte(k)})
	}
	sr, err := src.Pipeline(cmds...)
	if err != nil {
		return nil, err
	}
	dr, err := dst.Pipeline(cmds...)
	if err != nil {
		return nil, err
	}
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
		same, err := sameKey(src, dst, k)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", k, err)
		}
		if !same {
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

// verifyDelta: the keys marked since the precompare, then key counts and a sample.
func (s *Syncer) verifyDelta(src, dst *redisx.Conn, req VerifyReq) (map[string]*prefixReport, bool, error) {
	out := map[string]*prefixReport{}
	ok := true
	delta := s.deltaKeys()
	r := &prefixReport{Mode: "delta", SrcKeys: len(delta)}
	for i := 0; i < len(delta); i += verifyBatch {
		group := delta[i:min(i+verifyBatch, len(delta))]
		bad, err := s.compareKeys(src, dst, group)
		if err != nil {
			return nil, false, err
		}
		r.Compared += len(group)
		if len(bad) > 0 {
			ok = false
			r.Mismatched = append(r.Mismatched, bad[:min(20, len(bad))]...)
		}
	}
	out["delta"] = r
	for p, n := range req.Sample {
		pr := &prefixReport{Mode: fmt.Sprintf("count + sample %d", n)}
		sk, err := scanKeys(src, p, 0)
		if err != nil {
			return nil, false, err
		}
		dk, err := scanKeys(dst, p, 0)
		if err != nil {
			return nil, false, err
		}
		pr.SrcKeys, pr.DstKeys = len(sk), len(dk)
		if abs(len(sk)-len(dk)) > max(2, len(sk)/1000) {
			pr.CountDiff = len(dk) - len(sk)
			ok = false
		}
		step := max(1, len(sk)/max(1, n))
		var sample []string
		for i := 0; i < len(sk) && len(sample) < n; i += step {
			sample = append(sample, sk[i])
		}
		for i := 0; i < len(sample); i += verifyBatch {
			group := sample[i:min(i+verifyBatch, len(sample))]
			bad, err := s.compareKeys(src, dst, group)
			if err != nil {
				return nil, false, err
			}
			pr.Compared += len(group)
			if len(bad) > 0 {
				ok = false
				pr.Mismatched = append(pr.Mismatched, bad[:min(20, len(bad))]...)
			}
		}
		out[p] = pr
	}
	return out, ok, nil
}

func (s *Syncer) Verify(req VerifyReq) (map[string]*prefixReport, bool, error) {
	srcEP, dstEP, _ := s.dir()
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

	if req.Mode == "delta" {
		return s.verifyDelta(src, dst, req)
	}
	out := map[string]*prefixReport{}
	ok := true
	add := func(xs []string, x string) []string {
		if len(xs) < 20 {
			xs = append(xs, x)
		}
		return xs
	}
	for _, p := range req.Full {
		r := &prefixReport{Mode: "full"}
		sk, err := scanKeys(src, p, 0)
		if err != nil {
			return nil, false, err
		}
		dk, err := scanKeys(dst, p, 0)
		if err != nil {
			return nil, false, err
		}
		r.SrcKeys, r.DstKeys = len(sk), len(dk)
		inSrc := map[string]bool{}
		for _, k := range sk {
			inSrc[k] = true
		}
		for _, k := range dk {
			if !inSrc[k] {
				r.OnlyInDst = add(r.OnlyInDst, k)
				ok = false
			}
		}
		for i := 0; i < len(sk); i += verifyBatch {
			group := sk[i:min(i+verifyBatch, len(sk))]
			bad, err := s.compareKeys(src, dst, group)
			if err != nil {
				return nil, false, err
			}
			r.Compared += len(group)
			for _, k := range bad {
				r.Mismatched = add(r.Mismatched, k)
				ok = false
			}
		}
		out[p] = r
	}
	for p, n := range req.Sample {
		r := &prefixReport{Mode: fmt.Sprintf("sample %d", n)}
		sk, err := scanKeys(src, p, 0)
		if err != nil {
			return nil, false, err
		}
		dk, err := scanKeys(dst, p, 0)
		if err != nil {
			return nil, false, err
		}
		r.SrcKeys, r.DstKeys = len(sk), len(dk)
		if diff, tol := abs(len(sk)-len(dk)), max(2, len(sk)/100); diff > tol {
			r.CountDiff = len(dk) - len(sk)
			ok = false
		}
		if len(sk) > n {
			sk = sk[:n]
		}
		for i := 0; i < len(sk); i += verifyBatch {
			group := sk[i:min(i+verifyBatch, len(sk))]
			bad, err := s.compareKeys(src, dst, group)
			if err != nil {
				return nil, false, err
			}
			r.Compared += len(group)
			for _, k := range bad {
				r.Mismatched = add(r.Mismatched, k)
				ok = false
			}
		}
		out[p] = r
	}
	return out, ok, nil
}
