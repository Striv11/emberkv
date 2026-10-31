package db

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"
)

func scanAll(t *testing.T, d *DB, start, end []byte) (keys, vals []string) {
	t.Helper()
	err := d.Scan(start, end, func(k, v []byte) bool {
		keys, vals = append(keys, string(k)), append(vals, string(v))
		return true
	})
	if err != nil {
		t.Fatal(err)
	}
	return
}

func modelScan(m map[string]string, start, end []byte) (keys, vals []string) {
	for k := range m {
		if start != nil && k < string(start) || end != nil && k >= string(end) {
			continue
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		vals = append(vals, m[k])
	}
	return
}

func TestScanBasic(t *testing.T) {
	d := openSmall(t, t.TempDir(), 1<<20)
	defer d.Close()
	for _, k := range []string{"b", "a", "d", "c"} {
		d.Put([]byte(k), []byte("v"+k))
	}
	d.Flush()
	d.Put([]byte("b"), []byte("vb2"))
	d.Delete([]byte("c"))
	k, v := scanAll(t, d, nil, nil)
	if fmt.Sprint(k, v) != "[a b d] [va vb2 vd]" {
		t.Fatalf("%v %v", k, v)
	}
	k, _ = scanAll(t, d, []byte("b"), []byte("d"))
	if fmt.Sprint(k) != "[b]" {
		t.Fatalf("range: %v", k)
	}
	n := 0
	d.Scan(nil, nil, func(_, _ []byte) bool { n++; return false })
	if n != 1 {
		t.Fatalf("early stop visited %d", n)
	}
	d.Close()
	if err := d.Scan(nil, nil, func(_, _ []byte) bool { return true }); err != ErrClosed {
		t.Fatalf("closed: %v", err)
	}
}

func TestScanModelRandom(t *testing.T) {
	dir := t.TempDir()
	d := openSmall(t, dir, 2<<10)
	rng := rand.New(rand.NewSource(7))
	model := map[string]string{}
	for i := 0; i < 15000; i++ {
		k := fmt.Sprintf("key%03d", rng.Intn(300))
		switch rng.Intn(10) {
		case 0, 1, 2:
			d.Delete([]byte(k))
			delete(model, k)
		default:
			v := fmt.Sprintf("v%d", i)
			d.Put([]byte(k), []byte(v))
			model[k] = v
		}
		if i%500 == 499 {
			if rng.Intn(3) == 0 {
				d.Close()
				d = openSmall(t, dir, 2<<10)
			}
			var s, e []byte
			if rng.Intn(4) != 0 {
				s = []byte(fmt.Sprintf("key%03d", rng.Intn(300)))
			}
			if rng.Intn(4) != 0 {
				e = []byte(fmt.Sprintf("key%03d", rng.Intn(300)))
			}
			gk, gv := scanAll(t, d, s, e)
			wk, wv := modelScan(model, s, e)
			if fmt.Sprint(gk, gv) != fmt.Sprint(wk, wv) {
				t.Fatalf("op %d scan [%s,%s) mismatch:\n got %v\nwant %v", i, s, e, gk, wk)
			}
		}
	}
	d.Close()
}
