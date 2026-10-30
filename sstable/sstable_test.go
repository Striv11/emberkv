package sstable

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/Striv11/emberkv/memtable"
)

// buildEntries makes n user keys, each with 1-3 versions, in table order.
func buildEntries(rng *rand.Rand, n int) []memtable.Entry {
	var out []memtable.Entry
	seq := uint64(1000)
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("user/%06d/profile", i*2)) // gaps for absent-key probes
		versions := 1 + rng.Intn(3)
		for v := 0; v < versions; v++ {
			e := memtable.Entry{Key: key, Seq: seq - uint64(v), Kind: memtable.KindPut,
				Value: []byte(fmt.Sprintf("value-%d-%d", i, v))}
			if v == 0 && rng.Intn(5) == 0 {
				e.Kind, e.Value = memtable.KindDelete, nil
			}
			out = append(out, e)
		}
		seq += 10
	}
	return out
}

func write(t testing.TB, path string, entries []memtable.Entry) {
	t.Helper()
	w, err := NewWriter(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if err := w.Add(e); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Finish(); err != nil {
		t.Fatal(err)
	}
}

func TestRoundTripIteration(t *testing.T) {
	entries := buildEntries(rand.New(rand.NewSource(1)), 3000)
	path := filepath.Join(t.TempDir(), "t.sst")
	write(t, path, entries)
	r, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if r.Len() != len(entries) || len(r.index) < 10 {
		t.Fatalf("len=%d blocks=%d", r.Len(), len(r.index))
	}
	it := r.NewIterator()
	for i, want := range entries {
		if !it.Next() {
			t.Fatalf("ended early at %d: %v", i, it.Err())
		}
		got := it.Entry()
		if !bytes.Equal(got.Key, want.Key) || got.Seq != want.Seq || got.Kind != want.Kind ||
			!bytes.Equal(got.Value, want.Value) {
			t.Fatalf("entry %d: got %+v want %+v", i, got, want)
		}
	}
	if it.Next() || it.Err() != nil {
		t.Fatalf("expected clean end, err=%v", it.Err())
	}
}

func TestGetMatchesMemtable(t *testing.T) {
	rng := rand.New(rand.NewSource(2))
	entries := buildEntries(rng, 2000)
	mt := memtable.New()
	for _, e := range entries {
		mt.Add(e.Seq, e.Kind, e.Key, e.Value)
	}
	path := filepath.Join(t.TempDir(), "t.sst")
	write(t, path, entries)
	r, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	for i := 0; i < 20000; i++ {
		// Even ids exist, odd ids never do.
		key := []byte(fmt.Sprintf("user/%06d/profile", rng.Intn(4400)))
		seq := uint64(rng.Intn(1000 + 2000*10 + 50))
		wv, wf, wd := mt.Get(key, seq)
		gv, gf, gd, err := r.Get(key, seq)
		if err != nil {
			t.Fatal(err)
		}
		if gf != wf || gd != wd || !bytes.Equal(gv, wv) {
			t.Fatalf("Get(%s,%d): got (%q,%v,%v) want (%q,%v,%v)", key, seq, gv, gf, gd, wv, wf, wd)
		}
	}
	// Keys beyond the last block and before the first.
	for _, k := range []string{"", "a", "zzzz"} {
		if _, f, _, err := r.Get([]byte(k), 1<<60); f || err != nil {
			t.Fatalf("Get(%q) found=%v err=%v", k, f, err)
		}
	}
}

func TestSeek(t *testing.T) {
	entries := buildEntries(rand.New(rand.NewSource(3)), 1500)
	path := filepath.Join(t.TempDir(), "t.sst")
	write(t, path, entries)
	r, _ := Open(path)
	defer r.Close()

	rng := rand.New(rand.NewSource(4))
	targets := [][]byte{nil, []byte("user/"), []byte("zzz")}
	for i := 0; i < 300; i++ {
		targets = append(targets, []byte(fmt.Sprintf("user/%06d/profile", rng.Intn(3200))))
	}
	for _, target := range targets {
		start := sort.Search(len(entries), func(i int) bool { return bytes.Compare(entries[i].Key, target) >= 0 })
		it := r.NewIterator()
		it.Seek(target)
		n := 0
		for it.Next() {
			if start+n >= len(entries) || !bytes.Equal(it.Entry().Key, entries[start+n].Key) ||
				it.Entry().Seq != entries[start+n].Seq {
				t.Fatalf("seek %q: mismatch at +%d", target, n)
			}
			n++
		}
		if it.Err() != nil || start+n != len(entries) {
			t.Fatalf("seek %q: yielded %d, want %d (err=%v)", target, n, len(entries)-start, it.Err())
		}
	}
}

func TestVersionsOfKeyShareBlock(t *testing.T) {
	// One hot key with many large versions must not be split across blocks.
	var entries []memtable.Entry
	big := bytes.Repeat([]byte("x"), 1500)
	for s := uint64(50); s > 0; s-- {
		entries = append(entries, memtable.Entry{Key: []byte("hot"), Seq: s, Kind: memtable.KindPut, Value: big})
	}
	entries = append(entries, memtable.Entry{Key: []byte("next"), Seq: 1, Kind: memtable.KindPut, Value: []byte("n")})
	path := filepath.Join(t.TempDir(), "t.sst")
	write(t, path, entries)
	r, _ := Open(path)
	defer r.Close()
	for _, seq := range []uint64{1, 25, 50, 99} {
		v, f, d, err := r.Get([]byte("hot"), seq)
		if err != nil || !f || d || !bytes.Equal(v, big) {
			t.Fatalf("seq %d: found=%v deleted=%v err=%v", seq, f, d, err)
		}
	}
	if _, f, _, _ := r.Get([]byte("hot"), 0); f {
		t.Fatal("seq 0 should see nothing")
	}
}

func TestEmptyTable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "t.sst")
	write(t, path, nil)
	r, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if _, f, _, err := r.Get([]byte("k"), 10); f || err != nil {
		t.Fatalf("found=%v err=%v", f, err)
	}
	if r.NewIterator().Next() {
		t.Fatal("empty table yielded an entry")
	}
}

func TestWriterRejectsDisorder(t *testing.T) {
	dir := t.TempDir()
	cases := map[string][]memtable.Entry{
		"key descends": {{Key: []byte("b"), Seq: 2}, {Key: []byte("a"), Seq: 1}},
		"seq ascends":  {{Key: []byte("a"), Seq: 1}, {Key: []byte("a"), Seq: 2}},
		"duplicate":    {{Key: []byte("a"), Seq: 1}, {Key: []byte("a"), Seq: 1}},
	}
	for name, es := range cases {
		w, err := NewWriter(filepath.Join(dir, "x.sst"))
		if err != nil {
			t.Fatal(err)
		}
		var got error
		for _, e := range es {
			if err := w.Add(e); err != nil {
				got = err
			}
		}
		if got == nil {
			t.Errorf("%s: no error", name)
		}
		if w.Finish() == nil {
			t.Errorf("%s: Finish succeeded after failed Add", name)
		}
		w.Abort()
	}
}

func TestAbortLeavesNothing(t *testing.T) {
	dir := t.TempDir()
	w, _ := NewWriter(filepath.Join(dir, "x.sst"))
	w.Add(memtable.Entry{Key: []byte("a"), Seq: 1, Kind: memtable.KindPut})
	if err := w.Abort(); err != nil {
		t.Fatal(err)
	}
	if ents, _ := os.ReadDir(dir); len(ents) != 0 {
		t.Fatalf("leftover files: %v", ents)
	}
}

func TestBloomFalsePositiveRate(t *testing.T) {
	b := newBloom(10000, bloomBitsPerKy)
	for i := 0; i < 10000; i++ {
		b.add([]byte(fmt.Sprintf("in-%d", i)))
	}
	for i := 0; i < 10000; i++ {
		if !b.mayContain([]byte(fmt.Sprintf("in-%d", i))) {
			t.Fatal("false negative")
		}
	}
	fp := 0
	for i := 0; i < 20000; i++ {
		if b.mayContain([]byte(fmt.Sprintf("out-%d", i))) {
			fp++
		}
	}
	if rate := float64(fp) / 20000; rate > 0.03 {
		t.Fatalf("false positive rate %.3f too high", rate)
	}
}

func TestCorruptionRejected(t *testing.T) {
	entries := buildEntries(rand.New(rand.NewSource(5)), 500)
	dir := t.TempDir()
	path := filepath.Join(dir, "t.sst")
	write(t, path, entries)
	orig, _ := os.ReadFile(path)

	// Every single-byte flip must be detected: at open for footer, index and
	// bloom regions, otherwise when the damaged data block is read.
	for pos := 0; pos < len(orig); pos += 7 {
		bad := append([]byte(nil), orig...)
		bad[pos] ^= 0x40
		p := filepath.Join(dir, "bad.sst")
		os.WriteFile(p, bad, 0o644)
		r, err := Open(p)
		if err != nil {
			if !errors.Is(err, ErrCorrupt) {
				t.Fatalf("flip at %d: open error not ErrCorrupt: %v", pos, err)
			}
			continue
		}
		it := r.NewIterator()
		for it.Next() {
		}
		if !errors.Is(it.Err(), ErrCorrupt) {
			t.Fatalf("flip at %d went undetected (err=%v)", pos, it.Err())
		}
		r.Close()
	}

	// Every truncation must be rejected at open.
	for n := 0; n < len(orig); n += 13 {
		p := filepath.Join(dir, "short.sst")
		os.WriteFile(p, orig[:n], 0o644)
		if r, err := Open(p); err == nil {
			r.Close()
			t.Fatalf("truncation to %d bytes accepted", n)
		}
	}
	if _, err := Open(filepath.Join(dir, "missing.sst")); err == nil {
		t.Fatal("opened a missing file")
	}
}

func TestPrefixCompressionShrinksFile(t *testing.T) {
	entries := buildEntries(rand.New(rand.NewSource(6)), 2000)
	path := filepath.Join(t.TempDir(), "t.sst")
	write(t, path, entries)
	st, _ := os.Stat(path)
	raw := 0
	for _, e := range entries {
		raw += len(e.Key) + len(e.Value) + 9
	}
	if int(st.Size()) >= raw {
		t.Fatalf("file %d bytes not smaller than raw %d", st.Size(), raw)
	}
}

func BenchmarkGet(b *testing.B) {
	entries := buildEntries(rand.New(rand.NewSource(7)), 20000)
	path := filepath.Join(b.TempDir(), "t.sst")
	write(b, path, entries)
	r, _ := Open(path)
	defer r.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.Get([]byte(fmt.Sprintf("user/%06d/profile", (i%20000)*2)), 1<<60)
	}
}
