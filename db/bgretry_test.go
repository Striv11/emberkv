package db

import (
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Striv11/emberkv/vfs"
)

// failSSTSyncs fails the first n fsyncs of SSTable temp files.
func failSSTSyncs(fsys *vfs.MemFS, n int64) {
	var left atomic.Int64
	left.Store(n)
	fsys.SetFault(func(op, name string) error {
		if op == "sync" && strings.HasSuffix(name, ".sst.tmp") && left.Add(-1) >= 0 {
			return errInjected
		}
		return nil
	})
}

func fill(t *testing.T, d *DB, n int) map[string]string {
	t.Helper()
	model := map[string]string{}
	for i := 0; i < n; i++ {
		k, v := fmt.Sprintf("k%03d", i%120), fmt.Sprintf("v%d", i)
		if err := d.Put([]byte(k), []byte(v)); err != nil {
			t.Fatal(err)
		}
		model[k] = v
	}
	return model
}

// A transient error while building a flush's table must be retried, not
// turned into a sticky background error that rejects all later writes.
func TestBackgroundFlushRetriesTransientError(t *testing.T) {
	fsys := vfs.NewMem()
	d, err := Open("/db", Options{FS: fsys, Background: true, MemtableSize: 1 << 10, L0Trigger: 4,
		L1Size: 8 << 10, TargetFileSize: 1 << 10})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	failSSTSyncs(fsys, 2)
	model := fill(t, d, 800)
	if err := d.Flush(); err != nil {
		t.Fatalf("flush after transient errors: %v", err)
	}
	if err := d.WaitCompaction(); err != nil {
		t.Fatal(err)
	}
	if r := d.Stats().BGRetries; r == 0 {
		t.Fatal("expected at least one retry")
	}
	for k, want := range model {
		if got, err := d.Get([]byte(k)); err != nil || string(got) != want {
			t.Fatalf("Get(%s) = %q, %v; want %q", k, got, err, want)
		}
	}
	for _, e := range func() []string { l, _ := fsys.ReadDir("/db"); return l }() {
		if strings.HasSuffix(e, ".tmp") {
			t.Fatalf("leftover temp file %s", e)
		}
	}
}

// With retries disabled the same fault is sticky, as before.
func TestBackgroundFlushNoRetryIsSticky(t *testing.T) {
	fsys := vfs.NewMem()
	d, err := Open("/db", Options{FS: fsys, Background: true, MemtableSize: 1 << 10, BGRetries: -1})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	failSSTSyncs(fsys, 1)
	var werr error
	for i := 0; i < 2000 && werr == nil; i++ {
		werr = d.Put([]byte(fmt.Sprintf("k%04d", i)), []byte("value-value-value"))
	}
	if werr == nil {
		werr = d.Flush()
	}
	if werr == nil {
		t.Fatal("expected a sticky background error")
	}
}
