package memtable

import (
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"testing"
)

func TestNewestVersionWins(t *testing.T) {
	m := New()
	m.Add(1, KindPut, []byte("k"), []byte("v1"))
	m.Add(2, KindPut, []byte("k"), []byte("v2"))
	m.Add(3, KindDelete, []byte("k"), nil)
	m.Add(4, KindPut, []byte("k"), []byte("v4"))

	cases := []struct {
		seq     uint64
		want    string
		found   bool
		deleted bool
	}{
		{0, "", false, false},
		{1, "v1", true, false},
		{2, "v2", true, false},
		{3, "", true, true},
		{4, "v4", true, false},
		{100, "v4", true, false},
	}
	for _, c := range cases {
		v, found, deleted := m.Get([]byte("k"), c.seq)
		if found != c.found || deleted != c.deleted || string(v) != c.want {
			t.Errorf("seq %d: got (%q,%v,%v) want (%q,%v,%v)", c.seq, v, found, deleted, c.want, c.found, c.deleted)
		}
	}
	if _, found, _ := m.Get([]byte("missing"), 100); found {
		t.Error("missing key found")
	}
}

func TestCopiesInputs(t *testing.T) {
	m := New()
	k, v := []byte("key"), []byte("val")
	m.Add(1, KindPut, k, v)
	k[0], v[0] = 'X', 'X'
	got, found, _ := m.Get([]byte("key"), 1)
	if !found || string(got) != "val" {
		t.Fatalf("got %q %v", got, found)
	}
}

func TestIteratorOrder(t *testing.T) {
	m := New()
	rng := rand.New(rand.NewSource(7))
	type kv struct {
		k   string
		seq uint64
	}
	var want []kv
	for i := 1; i <= 2000; i++ {
		k := fmt.Sprintf("key%03d", rng.Intn(200))
		m.Add(uint64(i), KindPut, []byte(k), []byte("v"))
		want = append(want, kv{k, uint64(i)})
	}
	sort.Slice(want, func(i, j int) bool {
		if want[i].k != want[j].k {
			return want[i].k < want[j].k
		}
		return want[i].seq > want[j].seq
	})
	it := m.NewIterator()
	n := 0
	for ; it.Next(); n++ {
		e := it.Entry()
		if string(e.Key) != want[n].k || e.Seq != want[n].seq {
			t.Fatalf("entry %d: got %s/%d want %s/%d", n, e.Key, e.Seq, want[n].k, want[n].seq)
		}
	}
	if n != len(want) || m.Len() != len(want) {
		t.Fatalf("iterated %d, len %d, want %d", n, m.Len(), len(want))
	}
}

func TestRandomAgainstModel(t *testing.T) {
	m := New()
	model := map[string][]byte{}
	rng := rand.New(rand.NewSource(42))
	for seq := uint64(1); seq <= 20000; seq++ {
		k := fmt.Sprintf("k%d", rng.Intn(500))
		if rng.Intn(4) == 0 {
			m.Add(seq, KindDelete, []byte(k), nil)
			delete(model, k)
		} else {
			v := []byte(fmt.Sprintf("v%d", seq))
			m.Add(seq, KindPut, []byte(k), v)
			model[k] = v
		}
	}
	for i := 0; i < 500; i++ {
		k := fmt.Sprintf("k%d", i)
		v, found, deleted := m.Get([]byte(k), ^uint64(0))
		want, ok := model[k]
		live := found && !deleted
		if live != ok || (ok && !bytes.Equal(v, want)) {
			t.Fatalf("key %s: got (%q,%v,%v) want (%q,%v)", k, v, found, deleted, want, ok)
		}
	}
}

func TestEmptyIterator(t *testing.T) {
	if New().NewIterator().Next() {
		t.Fatal("empty table yielded entry")
	}
}

func TestIteratorFromSeeksToKey(t *testing.T) {
	m := New()
	m.Add(1, KindPut, []byte("a"), []byte("1"))
	m.Add(2, KindPut, []byte("c"), []byte("2"))
	m.Add(3, KindPut, []byte("c"), []byte("3"))
	m.Add(4, KindPut, []byte("e"), []byte("4"))
	it := m.NewIteratorFrom([]byte("b"))
	var got []uint64
	for it.Next() {
		got = append(got, it.Entry().Seq)
	}
	if len(got) != 3 || got[0] != 3 || got[1] != 2 || got[2] != 4 {
		t.Fatalf("got %v", got)
	}
	if NewIteratorFromEnd := m.NewIteratorFrom([]byte("z")); NewIteratorFromEnd.Next() {
		t.Fatal("seek past end yielded entry")
	}
}
