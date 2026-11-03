package db

import (
	"fmt"
	"testing"
)

func TestStatsCountersAndLevels(t *testing.T) {
	d, err := Open(t.TempDir(), Options{NoSync: true, MemtableSize: 2048, L0Trigger: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	for i := 0; i < 400; i++ {
		if err := d.Put([]byte(fmt.Sprintf("k%04d", i)), make([]byte, 64)); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 10; i++ {
		d.Delete([]byte(fmt.Sprintf("k%04d", i)))
	}
	d.Get([]byte("k0100"))
	d.Get([]byte("missing"))
	d.Scan(nil, nil, func(k, v []byte) bool { return false })

	s := d.Stats()
	if s.Puts != 400 || s.Deletes != 10 || s.Gets != 2 || s.Scans != 1 {
		t.Fatalf("op counters wrong: %+v", s)
	}
	if s.LastSeq != 410 {
		t.Fatalf("LastSeq = %d, want 410", s.LastSeq)
	}
	if s.Flushes == 0 || s.Compactions == 0 {
		t.Fatalf("expected flushes and compactions: %+v", s)
	}
	counts := d.LevelCounts()
	for l, c := range counts {
		if s.Levels[l].Tables != c {
			t.Fatalf("level %d tables = %d, want %d", l, s.Levels[l].Tables, c)
		}
		if (c > 0) != (s.Levels[l].Bytes > 0) {
			t.Fatalf("level %d: %d tables but %d bytes", l, c, s.Levels[l].Bytes)
		}
	}
}
