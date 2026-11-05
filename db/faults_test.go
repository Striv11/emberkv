package db

import (
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/Striv11/emberkv/vfs"
)

var errInjected = errors.New("injected I/O error")

// Random write/sync/syncdir failures: once an operation reports an error the
// DB may refuse further writes, but every write acknowledged before it must
// survive a power cut, and the failed write may be either present or absent.
func TestInjectedIOErrorsNeverLoseAcknowledgedWrites(t *testing.T) {
	for _, bg := range []bool{false, true} {
		t.Run(fmt.Sprintf("background=%v", bg), func(t *testing.T) {
			rng := rand.New(rand.NewSource(11))
			fsys := vfs.NewMem()
			model := map[string]string{}
			sawFailure := 0
			for round := 0; round < 60; round++ {
				opts := Options{FS: fsys, MemtableSize: 1 << 10, L0Trigger: 2, L1Size: 4 << 10,
					TargetFileSize: 1 << 10, Background: bg}
				d, err := Open("/db", opts)
				if err != nil {
					t.Fatalf("round %d open: %v", round, err)
				}
				if got := dumpState(t, d); !equal(got, model) {
					t.Fatalf("round %d: state mismatch\n%s", round, diff(got, model))
				}
				// Arm faults after a calm start; ~1.5% of I/O calls fail.
				frng := rand.New(rand.NewSource(int64(round))) // not shared with the test body
				var mu sync.Mutex                              // background goroutines call the injector too
				fsys.SetFault(func(op, name string) error {
					mu.Lock()
					defer mu.Unlock()
					if frng.Intn(70) == 0 {
						return errInjected
					}
					return nil
				})
				var inflight map[string]string // model if the failed op took effect
				for i := 0; i < 400 && inflight == nil; i++ {
					k := fmt.Sprintf("k%03d", rng.Intn(60))
					var err error
					alt := copyMap(model)
					if rng.Intn(4) == 0 {
						err = d.Delete([]byte(k))
						delete(alt, k)
					} else {
						v := fmt.Sprintf("v%d-%d", round, i)
						err = d.Put([]byte(k), []byte(v))
						alt[k] = v
					}
					if err != nil {
						inflight = alt
						sawFailure++
						break
					}
					model = alt
				}
				fsys.SetFault(nil)
				next := fsys.Crash(rng)
				d.Close()
				fsys = next
				if inflight != nil {
					// Resolve the ambiguity by looking at what recovered.
					d, err := Open("/db", Options{FS: fsys, MemtableSize: 1 << 10, L0Trigger: 2, L1Size: 4 << 10, TargetFileSize: 1 << 10})
					if err != nil {
						t.Fatalf("round %d reopen: %v", round, err)
					}
					got := dumpState(t, d)
					d.Close()
					switch {
					case equal(got, model):
					case equal(got, inflight):
						model = inflight
					default:
						t.Fatalf("round %d: recovered state is neither pre- nor post-failure\n%s", round, diff(got, model))
					}
				}
			}
			if sawFailure == 0 {
				t.Fatal("fault injector never fired")
			}
		})
	}
}

func copyMap(m map[string]string) map[string]string {
	c := make(map[string]string, len(m))
	for k, v := range m {
		c[k] = v
	}
	return c
}
