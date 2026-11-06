package db

import (
	"errors"
	"fmt"
	"math/rand"
	"testing"

	"github.com/Striv11/emberkv/vfs"
)

// A failing disk read must surface as an error. It must never turn into a
// missing key (ErrNotFound), a wrong value, or a silently shortened Scan.
// The block cache is disabled so every lookup goes to the file.
func TestReadFaultsSurfaceAsErrors(t *testing.T) {
	fsys := vfs.NewMem()
	d, err := Open("/db", Options{FS: fsys, MemtableSize: 1 << 10, L0Trigger: 4, L1Size: 8 << 10,
		TargetFileSize: 1 << 10, BlockCacheSize: -1})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	model := map[string]string{}
	for i := 0; i < 600; i++ {
		k, v := fmt.Sprintf("k%03d", i%150), fmt.Sprintf("v%d", i)
		if err := d.Put([]byte(k), []byte(v)); err != nil {
			t.Fatal(err)
		}
		model[k] = v
	}
	if err := d.Flush(); err != nil {
		t.Fatal(err)
	}
	if c := d.LevelCounts(); c[0]+c[1] == 0 {
		t.Fatalf("expected tables on disk, got %v", c)
	}

	rng := rand.New(rand.NewSource(3))
	fsys.SetFault(func(op, name string) error {
		if op == "read" && rng.Intn(3) == 0 { // reads here are serialized by the DB read lock
			return errInjected
		}
		return nil
	})
	var getErrs, getOK, scanErrs, scanOK int
	for i := 0; i < 600; i++ {
		k := fmt.Sprintf("k%03d", rng.Intn(150))
		v, err := d.Get([]byte(k))
		switch {
		case err == nil:
			getOK++
			if string(v) != model[k] {
				t.Fatalf("Get(%s) = %q, want %q", k, v, model[k])
			}
		case errors.Is(err, errInjected):
			getErrs++
		default:
			t.Fatalf("Get(%s): %v (a read fault must not look like a miss)", k, err)
		}
	}
	for i := 0; i < 40; i++ {
		got := map[string]string{}
		err := d.Scan(nil, nil, func(k, v []byte) bool { got[string(k)] = string(v); return true })
		switch {
		case err == nil:
			scanOK++
			if !equal(got, model) {
				t.Fatalf("Scan succeeded with wrong contents\n%s", diff(got, model))
			}
		case errors.Is(err, errInjected):
			scanErrs++
		default:
			t.Fatalf("Scan: %v", err)
		}
	}
	if getErrs == 0 || getOK == 0 || scanErrs == 0 {
		t.Fatalf("weak coverage: getErrs=%d getOK=%d scanErrs=%d scanOK=%d", getErrs, getOK, scanErrs, scanOK)
	}

	// The faults were transient: with them cleared everything is intact.
	fsys.SetFault(nil)
	if got := dumpState(t, d); !equal(got, model) {
		t.Fatalf("state damaged after read faults\n%s", diff(got, model))
	}
}
