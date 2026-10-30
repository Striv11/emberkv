package manifest

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestReplayAndRemove(t *testing.T) {
	p := filepath.Join(t.TempDir(), "MANIFEST")
	m, err := Open(p)
	if err != nil {
		t.Fatal(err)
	}
	m.Apply(Edit{LogNum: 3, NextFile: 4, LastSeq: 10, Added: []FileMeta{{Num: 2, Size: 99, Smallest: []byte("a"), Largest: []byte("m")}}})
	m.Apply(Edit{LogNum: 5, NextFile: 7, LastSeq: 20, Added: []FileMeta{{Num: 6, Level: 0}, {Num: 4, Level: 1}}, Removed: []uint64{2}})
	want := m.State()
	m.Close()

	m, err = Open(p)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	got := m.State()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v want %+v", got, want)
	}
	if got.LogNum != 5 || got.NextFile != 7 || got.LastSeq != 20 || len(got.Files) != 2 {
		t.Fatalf("bad state %+v", got)
	}
	s := got.Sorted()
	if s[0].Num != 6 || s[1].Num != 4 {
		t.Fatalf("sort order: %+v", s)
	}
}

func TestTornTailDropsWholeEdit(t *testing.T) {
	p := filepath.Join(t.TempDir(), "MANIFEST")
	m, _ := Open(p)
	m.Apply(Edit{NextFile: 2, Added: []FileMeta{{Num: 1}}})
	m.Apply(Edit{NextFile: 3, Added: []FileMeta{{Num: 2}}, Removed: []uint64{1}})
	m.Close()
	st, _ := os.Stat(p)
	for cut := int64(1); cut < 20; cut++ {
		os.WriteFile(p+".c", nil, 0o644)
		data, _ := os.ReadFile(p)
		os.WriteFile(p+".c", data[:st.Size()-cut], 0o644)
		m, err := Open(p + ".c")
		if err != nil {
			t.Fatal(err)
		}
		s := m.State()
		// Edit 2 is all-or-nothing: file 1 present XOR file 2 present.
		_, has1 := s.Files[1]
		_, has2 := s.Files[2]
		if has1 == has2 {
			t.Fatalf("cut %d: non-atomic state %+v", cut, s.Files)
		}
		m.Close()
	}
}

func TestSnapshotRewrite(t *testing.T) {
	p := filepath.Join(t.TempDir(), "MANIFEST")
	m, _ := Open(p)
	for i := uint64(1); i <= snapshotThreshold+50; i++ {
		e := Edit{NextFile: i + 1, LastSeq: i, Added: []FileMeta{{Num: i}}}
		if i > 1 {
			e.Removed = []uint64{i - 1}
		}
		if err := m.Apply(e); err != nil {
			t.Fatal(err)
		}
	}
	want := m.State()
	m.Close()
	before, _ := os.Stat(p)

	m, err := Open(p)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(m.State(), want) {
		t.Fatal("state changed by rewrite")
	}
	m.Apply(Edit{NextFile: 1000})
	m.Close()
	after, _ := os.Stat(p)
	if after.Size() >= before.Size() {
		t.Fatalf("manifest not compacted: %d -> %d", before.Size(), after.Size())
	}
	m, _ = Open(p)
	defer m.Close()
	if s := m.State(); s.NextFile != 1000 || len(s.Files) != 1 {
		t.Fatalf("bad state after rewrite+append: %+v", s)
	}
}

func TestDecodeRejectsGarbage(t *testing.T) {
	for _, p := range [][]byte{{}, {1, 2}, {0, 0, 0, 255, 255, 255}, {0, 0, 0, 0, 0, 7}} {
		if _, err := decode(p); err == nil {
			t.Fatalf("accepted %v", p)
		}
	}
}
