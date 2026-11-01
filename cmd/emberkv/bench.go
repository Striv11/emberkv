package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/Striv11/emberkv/db"
)

func runBench(args []string) error {
	fs := flag.NewFlagSet("bench", flag.ExitOnError)
	n := fs.Int("n", 100000, "number of keys")
	vsize := fs.Int("value", 100, "value size in bytes")
	sync := fs.Bool("sync", false, "fsync every write (slow)")
	dir := fs.String("dir", "", "database directory (default: temp dir)")
	fs.Parse(args)
	if *dir == "" {
		tmp, err := os.MkdirTemp("", "emberkv-bench-")
		if err != nil {
			return err
		}
		defer os.RemoveAll(tmp)
		*dir = tmp
	}
	d, err := db.Open(*dir, db.Options{NoSync: !*sync})
	if err != nil {
		return err
	}
	defer d.Close()

	val := make([]byte, *vsize)
	for i := range val {
		val[i] = byte('a' + i%26)
	}
	report := func(name string, ops int, el time.Duration) {
		fmt.Printf("%-12s %9d ops  %8.0f ops/s  %7.2f us/op  %6.1f MB/s\n", name, ops,
			float64(ops)/el.Seconds(), float64(el.Microseconds())/float64(ops),
			float64(ops)*float64(*vsize+16)/el.Seconds()/1e6)
	}

	start := time.Now()
	for i := 0; i < *n; i++ {
		if err := d.Put([]byte(fmt.Sprintf("key%012d", i)), val); err != nil {
			return err
		}
	}
	report("fillseq", *n, time.Since(start))

	rng := rand.New(rand.NewSource(1))
	start = time.Now()
	for i := 0; i < *n; i++ {
		if _, err := d.Get([]byte(fmt.Sprintf("key%012d", rng.Intn(*n)))); err != nil {
			return fmt.Errorf("readrandom: %w", err)
		}
	}
	report("readrandom", *n, time.Since(start))

	start = time.Now()
	count := 0
	if err := d.Scan(nil, nil, func(k, v []byte) bool { count++; return true }); err != nil {
		return err
	}
	report("scan", count, time.Since(start))
	return nil
}
