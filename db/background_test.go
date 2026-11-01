package db

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
)

func bgOpts() Options {
	o := smallOpts()
	o.Background = true
	return o
}

func TestBackgroundCompactionMovesData(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(dir, bgOpts())
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3000; i++ {
		if err := d.Put([]byte(fmt.Sprintf("key%05d", i)), []byte(fmt.Sprintf("v%d-xxxxxxxxxxxxxx", i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := d.WaitCompaction(); err != nil {
		t.Fatal(err)
	}
	c := d.LevelCounts()
	if c[0] >= 3 || c[1]+c[2]+c[3] == 0 {
		t.Fatalf("expected data below L0: %v", c)
	}
	checkLevelInvariants(t, d)
	d.Close()
	d, err = Open(dir, bgOpts())
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	for i := 0; i < 3000; i++ {
		v, err := d.Get([]byte(fmt.Sprintf("key%05d", i)))
		if err != nil || string(v) != fmt.Sprintf("v%d-xxxxxxxxxxxxxx", i) {
			t.Fatalf("key%05d: %q %v", i, v, err)
		}
	}
}

// Model check with concurrent readers while the compactor runs and reopens.
func TestBackgroundCompactionModel(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(dir, bgOpts())
	if err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewSource(7))
	model := map[string]string{}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for r := 0; r < 3; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				d.Get([]byte(fmt.Sprintf("k%03d", rand.Intn(300))))
				d.Scan([]byte("k100"), []byte("k150"), func(k, v []byte) bool { return true })
			}
		}()
	}
	for i := 0; i < 15000; i++ {
		k := fmt.Sprintf("k%03d", rng.Intn(300))
		if rng.Intn(4) == 0 {
			if err := d.Delete([]byte(k)); err != nil {
				t.Fatal(err)
			}
			delete(model, k)
		} else {
			v := fmt.Sprintf("val-%d-xxxxxxxxxx", i)
			if err := d.Put([]byte(k), []byte(v)); err != nil {
				t.Fatal(err)
			}
			model[k] = v
		}
		if i%3000 == 2999 {
			close(stop)
			wg.Wait()
			if err := d.Close(); err != nil {
				t.Fatal(err)
			}
			if d, err = Open(dir, bgOpts()); err != nil {
				t.Fatal(err)
			}
			stop = make(chan struct{})
			for r := 0; r < 3; r++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for {
						select {
						case <-stop:
							return
						default:
						}
						d.Get([]byte("k001"))
					}
				}()
			}
		}
	}
	close(stop)
	wg.Wait()
	if err := d.WaitCompaction(); err != nil {
		t.Fatal(err)
	}
	checkLevelInvariants(t, d)
	got := map[string]string{}
	if err := d.Scan(nil, nil, func(k, v []byte) bool { got[string(k)] = string(v); return true }); err != nil {
		t.Fatal(err)
	}
	if len(got) != len(model) {
		t.Fatalf("scan has %d keys, model %d", len(got), len(model))
	}
	for k, v := range model {
		if got[k] != v {
			t.Fatalf("%s: %q want %q", k, got[k], v)
		}
	}
	d.Close()
}

func TestBackgroundCloseWhileCompacting(t *testing.T) {
	for i := 0; i < 20; i++ {
		dir := t.TempDir()
		d, err := Open(dir, bgOpts())
		if err != nil {
			t.Fatal(err)
		}
		for j := 0; j < 800; j++ {
			d.Put([]byte(fmt.Sprintf("key%05d", j)), []byte("xxxxxxxxxxxxxxxxxxxxxxxx"))
		}
		if err := d.Close(); err != nil {
			t.Fatal(err)
		}
		d, err = Open(dir, bgOpts())
		if err != nil {
			t.Fatal(err)
		}
		for j := 0; j < 800; j += 37 {
			if _, err := d.Get([]byte(fmt.Sprintf("key%05d", j))); err != nil {
				t.Fatalf("iter %d key %d: %v", i, j, err)
			}
		}
		d.Close()
	}
}
