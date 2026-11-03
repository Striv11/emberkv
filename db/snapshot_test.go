package db

import (
	"fmt"
	"math/rand"
	"testing"
)

func TestSnapshotIsolation(t *testing.T) {
	d, err := Open(t.TempDir(), Options{NoSync: true, MemtableSize: 512, L0Trigger: 2, L1Size: 2048, TargetFileSize: 512})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	d.Put([]byte("a"), []byte("1"))
	d.Put([]byte("b"), []byte("1"))
	s, _ := d.NewSnapshot()
	d.Put([]byte("a"), []byte("2"))
	d.Delete([]byte("b"))
	d.Put([]byte("c"), []byte("2"))
	if v, err := s.Get([]byte("a")); err != nil || string(v) != "1" {
		t.Fatalf("a=%q %v", v, err)
	}
	if v, err := s.Get([]byte("b")); err != nil || string(v) != "1" {
		t.Fatalf("b=%q %v", v, err)
	}
	if _, err := s.Get([]byte("c")); err != ErrNotFound {
		t.Fatalf("c: %v", err)
	}
	var got []string
	s.Scan(nil, nil, func(k, v []byte) bool { got = append(got, string(k)+v2(v)); return true })
	if fmt.Sprint(got) != "[a1 b1]" {
		t.Fatalf("scan %v", got)
	}
	s.Release()
	s.Release() // idempotent
	if len(d.snaps) != 0 {
		t.Fatal("snapshot leaked")
	}
}

func v2(v []byte) string { return string(v) }

// Snapshots must keep seeing their data through flushes and compactions,
// including bottom-level tombstone drops, in both sync and background modes.
func TestSnapshotSurvivesCompaction(t *testing.T) {
	for _, bg := range []bool{false, true} {
		t.Run(fmt.Sprint("bg=", bg), func(t *testing.T) {
			d, err := Open(t.TempDir(), Options{NoSync: true, MemtableSize: 400, L0Trigger: 2, L1Size: 1500, TargetFileSize: 400, Background: bg})
			if err != nil {
				t.Fatal(err)
			}
			defer d.Close()
			rng := rand.New(rand.NewSource(7))
			model := map[string]string{}
			type snapModel struct {
				s *Snapshot
				m map[string]string
			}
			var snaps []snapModel
			for i := 0; i < 6000; i++ {
				k := fmt.Sprintf("k%03d", rng.Intn(120))
				if rng.Intn(4) == 0 {
					d.Delete([]byte(k))
					delete(model, k)
				} else {
					v := fmt.Sprintf("v%d", i)
					d.Put([]byte(k), []byte(v))
					model[k] = v
				}
				if i%500 == 0 {
					s, _ := d.NewSnapshot()
					m := map[string]string{}
					for k, v := range model {
						m[k] = v
					}
					snaps = append(snaps, snapModel{s, m})
				}
				if i%1500 == 1499 && len(snaps) > 0 { // release the oldest
					snaps[0].s.Release()
					snaps = snaps[1:]
				}
			}
			d.Flush()
			if err := d.WaitCompaction(); err != nil {
				t.Fatal(err)
			}
			for _, sm := range snaps {
				got := map[string]string{}
				sm.s.Scan(nil, nil, func(k, v []byte) bool { got[string(k)] = string(v); return true })
				if fmt.Sprint(got) != fmt.Sprint(sm.m) {
					t.Fatalf("snapshot seq %d diverged:\n got %v\nwant %v", sm.s.Seq(), got, sm.m)
				}
				for k, v := range sm.m {
					if g, err := sm.s.Get([]byte(k)); err != nil || string(g) != v {
						t.Fatalf("Get %s = %q,%v want %q", k, g, err, v)
					}
				}
			}
			// The live view is unaffected by retention.
			got := map[string]string{}
			d.Scan(nil, nil, func(k, v []byte) bool { got[string(k)] = string(v); return true })
			if fmt.Sprint(got) != fmt.Sprint(model) {
				t.Fatal("live view diverged")
			}
		})
	}
}

// Without snapshots compaction still reclaims old versions and tombstones.
func TestNoSnapshotReclaims(t *testing.T) {
	d, _ := Open(t.TempDir(), Options{NoSync: true, L0Trigger: 2})
	defer d.Close()
	for i := 0; i < 3; i++ {
		d.Put([]byte("x"), []byte(fmt.Sprint(i)))
		d.Flush()
	}
	d.Delete([]byte("x"))
	d.Flush()
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.maybeCompactLocked(); err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, f := range d.allTables() {
		it := d.tables[f].NewIterator()
		for it.Next() {
			n++
		}
	}
	if n != 0 {
		t.Fatalf("%d entries left, want 0", n)
	}
}

func TestSnapshotClosed(t *testing.T) {
	d, _ := Open(t.TempDir(), Options{NoSync: true})
	s, _ := d.NewSnapshot()
	d.Close()
	if _, err := s.Get([]byte("a")); err != ErrClosed {
		t.Fatal(err)
	}
	if _, err := d.NewSnapshot(); err != ErrClosed {
		t.Fatal(err)
	}
	s.Release()
}
