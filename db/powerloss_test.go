package db

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/Striv11/emberkv/vfs"
)

func dumpState(t *testing.T, d *DB) map[string]string {
	t.Helper()
	got := map[string]string{}
	if err := d.Scan(nil, nil, func(k, v []byte) bool { got[string(k)] = string(v); return true }); err != nil {
		t.Fatal(err)
	}
	return got
}

// Every acknowledged (fsynced) write must survive a simulated power cut:
// unsynced file bytes and unsynced directory entries are dropped, and a random
// part of each unsynced tail may be kept (torn writes).
func TestPowerLossKeepsAcknowledgedWrites(t *testing.T) {
	for _, bg := range []bool{false, true} {
		t.Run(fmt.Sprintf("background=%v", bg), func(t *testing.T) {
			rng := rand.New(rand.NewSource(7))
			fsys := vfs.NewMem()
			opts := Options{
				FS: fsys, MemtableSize: 2 << 10, L0Trigger: 2, L1Size: 8 << 10,
				TargetFileSize: 2 << 10, Background: bg,
			}
			model := map[string]string{}
			ops := 0
			for round := 0; round < 40; round++ {
				opts.FS = fsys
				d, err := Open("/db", opts)
				if err != nil {
					t.Fatalf("round %d open: %v", round, err)
				}
				if got := dumpState(t, d); !equal(got, model) {
					t.Fatalf("round %d: recovered %d keys, want %d\n%s", round, len(got), len(model), diff(got, model))
				}
				for i, n := 0, 20+rng.Intn(300); i < n; i++ {
					k := fmt.Sprintf("k%03d", rng.Intn(120))
					if rng.Intn(4) == 0 {
						if err := d.Delete([]byte(k)); err != nil {
							t.Fatal(err)
						}
						delete(model, k)
					} else {
						v := fmt.Sprintf("v%d-%s", ops, k)
						if err := d.Put([]byte(k), []byte(v)); err != nil {
							t.Fatal(err)
						}
						model[k] = v
					}
					ops++
				}
				// Power cut while background work may be mid-flight.
				next := fsys.Crash(rng)
				d.Close()
				fsys = next
			}
			if ops == 0 {
				t.Fatal("no ops")
			}
		})
	}
}

func equal(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

func diff(got, want map[string]string) string {
	s := ""
	for k, v := range want {
		if g, ok := got[k]; !ok || g != v {
			s += fmt.Sprintf("  want %s=%q got %q (present=%v)\n", k, v, g, ok)
		}
	}
	for k := range got {
		if _, ok := want[k]; !ok {
			s += fmt.Sprintf("  unexpected %s\n", k)
		}
	}
	return s
}
