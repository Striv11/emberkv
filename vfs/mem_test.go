package vfs

import (
	"io"
	"math/rand"
	"os"
	"testing"
)

func write(t *testing.T, f File, s string) {
	t.Helper()
	if _, err := f.Write([]byte(s)); err != nil {
		t.Fatal(err)
	}
}

func read(t *testing.T, m *MemFS, name string) (string, bool) {
	t.Helper()
	f, err := m.Open(name)
	if err != nil {
		return "", false
	}
	b, _ := io.ReadAll(f)
	return string(b), true
}

func TestCrashDropsUnsyncedBytes(t *testing.T) {
	m := NewMem()
	f, _ := m.OpenFile("/d/a", os.O_RDWR|os.O_CREATE, 0o644)
	write(t, f, "hello")
	f.Sync()
	write(t, f, " world")
	m.SyncDir("/d")
	got, ok := read(t, m.Crash(nil), "/d/a")
	if !ok || got != "hello" {
		t.Fatalf("got %q %v, want hello", got, ok)
	}
}

func TestCrashDropsUnsyncedNames(t *testing.T) {
	m := NewMem()
	f, _ := m.OpenFile("/d/a", os.O_RDWR|os.O_CREATE, 0o644)
	write(t, f, "x")
	f.Sync()
	if _, ok := read(t, m.Crash(nil), "/d/a"); ok {
		t.Fatal("file survived without SyncDir")
	}
	m.SyncDir("/d")
	g, _ := m.OpenFile("/d/b", os.O_RDWR|os.O_CREATE, 0o644)
	write(t, g, "y")
	g.Sync()
	m.Rename("/d/a", "/d/c")
	m.Remove("/d/b")
	c := m.Crash(nil)
	if s, ok := read(t, c, "/d/a"); !ok || s != "x" {
		t.Fatalf("unsynced rename took effect: %q %v", s, ok)
	}
	if _, ok := read(t, c, "/d/c"); ok {
		t.Fatal("renamed name survived")
	}
}

func TestCrashTornTailIsPrefix(t *testing.T) {
	m := NewMem()
	f, _ := m.OpenFile("/d/a", os.O_RDWR|os.O_CREATE, 0o644)
	write(t, f, "abc")
	f.Sync()
	write(t, f, "defghij")
	m.SyncDir("/d")
	rng := rand.New(rand.NewSource(1))
	sawTorn := false
	for i := 0; i < 50; i++ {
		s, _ := read(t, m.Crash(rng), "/d/a")
		if len(s) < 3 || s != "abcdefghij"[:len(s)] {
			t.Fatalf("bad survivor %q", s)
		}
		if len(s) > 3 && len(s) < 10 {
			sawTorn = true
		}
	}
	if !sawTorn {
		t.Fatal("never kept a partial tail")
	}
}

func TestReadDirAndStat(t *testing.T) {
	m := NewMem()
	for _, n := range []string{"/d/b", "/d/a", "/e/z"} {
		f, _ := m.OpenFile(n, os.O_RDWR|os.O_CREATE, 0o644)
		write(t, f, "12")
	}
	names, _ := m.ReadDir("/d")
	if len(names) != 2 || names[0] != "a" || names[1] != "b" {
		t.Fatalf("names = %v", names)
	}
	if st, err := m.Stat("/d/a"); err != nil || st.Size() != 2 {
		t.Fatalf("stat: %v %v", st, err)
	}
	if _, err := m.Stat("/nope"); !os.IsNotExist(err) {
		t.Fatalf("want not-exist, got %v", err)
	}
}
