package merge

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/Striv11/emberkv/memtable"
)

type sliceSrc struct {
	ents []memtable.Entry
	i    int
	err  error
}

func (s *sliceSrc) Next() bool {
	if s.i >= len(s.ents) {
		return false
	}
	s.i++
	return true
}
func (s *sliceSrc) Entry() memtable.Entry { return s.ents[s.i-1] }
func (s *sliceSrc) Err() error {
	if s.i >= len(s.ents) {
		return s.err
	}
	return nil
}

func ent(k string, seq uint64, kind memtable.Kind, v string) memtable.Entry {
	return memtable.Entry{Key: []byte(k), Value: []byte(v), Seq: seq, Kind: kind}
}

func TestMergeOrder(t *testing.T) {
	a := &sliceSrc{ents: []memtable.Entry{ent("a", 5, memtable.KindPut, ""), ent("c", 1, memtable.KindPut, "")}}
	b := &sliceSrc{ents: []memtable.Entry{ent("a", 9, memtable.KindPut, ""), ent("b", 2, memtable.KindPut, "")}}
	empty := &sliceSrc{}
	it := New(a, empty, b)
	var got []string
	for it.Next() {
		e := it.Entry()
		got = append(got, fmt.Sprintf("%s@%d", e.Key, e.Seq))
	}
	want := "[a@9 a@5 b@2 c@1]"
	if fmt.Sprint(got) != want {
		t.Fatalf("got %v want %s", got, want)
	}
}

func TestUserNewestWinsAndTombstones(t *testing.T) {
	old := &sliceSrc{ents: []memtable.Entry{
		ent("a", 1, memtable.KindPut, "a1"), ent("b", 2, memtable.KindPut, "b2"), ent("c", 3, memtable.KindPut, "c3"),
	}}
	newer := &sliceSrc{ents: []memtable.Entry{
		ent("a", 7, memtable.KindPut, "a7"), ent("b", 8, memtable.KindDelete, ""), ent("d", 9, memtable.KindPut, "d9"),
	}}
	collect := func(snap uint64, end []byte) string {
		o, n := *old, *newer
		o.i, n.i = 0, 0
		u := NewUser(New(&n, &o), snap, end)
		var s string
		for u.Next() {
			s += fmt.Sprintf("%s=%s ", u.Key(), u.Value())
		}
		if u.Err() != nil {
			t.Fatal(u.Err())
		}
		return s
	}
	if g := collect(100, nil); g != "a=a7 c=c3 d=d9 " {
		t.Fatalf("latest: %q", g)
	}
	if g := collect(5, nil); g != "a=a1 b=b2 c=c3 " {
		t.Fatalf("snapshot 5: %q", g)
	}
	if g := collect(100, []byte("c")); g != "a=a7 " {
		t.Fatalf("end exclusive: %q", g)
	}
}

func TestSourceErrorPropagates(t *testing.T) {
	bad := &sliceSrc{ents: []memtable.Entry{ent("a", 1, memtable.KindPut, "")}, err: fmt.Errorf("boom")}
	u := NewUser(New(bad), 10, nil)
	for u.Next() {
	}
	if u.Err() == nil {
		t.Fatal("error lost")
	}
}

func TestRandomMergeMatchesSort(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	var all []memtable.Entry
	var srcs []Source
	seq := uint64(0)
	for s := 0; s < 6; s++ {
		var es []memtable.Entry
		for i := 0; i < 200; i++ {
			seq++
			es = append(es, ent(fmt.Sprintf("k%03d", rng.Intn(80)), seq, memtable.KindPut, ""))
		}
		sort.Slice(es, func(i, j int) bool { return before(es[i], es[j]) })
		all = append(all, es...)
		srcs = append(srcs, &sliceSrc{ents: es})
	}
	sort.Slice(all, func(i, j int) bool { return before(all[i], all[j]) })
	it := New(srcs...)
	for i := 0; it.Next(); i++ {
		if e := it.Entry(); string(e.Key) != string(all[i].Key) || e.Seq != all[i].Seq {
			t.Fatalf("pos %d: %s@%d want %s@%d", i, e.Key, e.Seq, all[i].Key, all[i].Seq)
		}
	}
}
