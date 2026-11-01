package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/Striv11/emberkv/db"
)

// The crash workload is a deterministic sequence of operations indexed by i.
// Op i deletes key(i-3) when i%7 == 6 and otherwise puts key(i) = value(i).
// The writer prints "ack i" after op i returns, so every acked op must be
// durable. Ops run strictly one at a time, so after a kill the recovered
// state must equal the model after exactly N or N+1 ops, where N is the
// number of acked ops (the +1 is the op that was in flight).

func crashOptions() db.Options {
	// Tiny thresholds so kills land during flushes and compactions too.
	return db.Options{MemtableSize: 16 << 10, L0Trigger: 2, L1Size: 64 << 10, TargetFileSize: 16 << 10, Background: true}
}

func ckey(i int) []byte { return []byte(fmt.Sprintf("key%08d", i)) }

func cval(i int) []byte {
	return bytes.Repeat([]byte(fmt.Sprintf("v%d;", i)), 8+i%13)
}

func applyOp(m map[string][]byte, i int) {
	if i%7 == 6 {
		delete(m, string(ckey(i-3)))
	} else {
		m[string(ckey(i))] = cval(i)
	}
}

func runCrashWriter(args []string) error {
	fs := flag.NewFlagSet("crashwriter", flag.ExitOnError)
	dir := fs.String("dir", "", "database directory")
	start := fs.Int("start", 0, "first op index")
	fs.Parse(args)
	d, err := db.Open(*dir, crashOptions())
	if err != nil {
		return err
	}
	for i := *start; ; i++ {
		if i%7 == 6 {
			err = d.Delete(ckey(i - 3))
		} else {
			err = d.Put(ckey(i), cval(i))
		}
		if err != nil {
			return err
		}
		fmt.Fprintf(os.Stdout, "ack %d\n", i)
	}
}

func runCrashTest(args []string) error {
	fs := flag.NewFlagSet("crashtest", flag.ExitOnError)
	iters := fs.Int("iters", 100, "number of kill/recover cycles")
	seed := fs.Int64("seed", time.Now().UnixNano(), "random seed")
	dir := fs.String("dir", "", "database directory (default: temp dir)")
	fs.Parse(args)
	if *dir == "" {
		tmp, err := os.MkdirTemp("", "emberkv-crash-")
		if err != nil {
			return err
		}
		defer os.RemoveAll(tmp)
		*dir = tmp
	}
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	rng := rand.New(rand.NewSource(*seed))
	fmt.Printf("crashtest: %d iterations, seed %d, dir %s\n", *iters, *seed, *dir)

	next := 0 // number of ops known to be applied
	totalAcked := 0
	for it := 1; it <= *iters; it++ {
		acked, err := runAndKill(exe, *dir, next, time.Duration(20+rng.Intn(280))*time.Millisecond)
		if err != nil {
			return fmt.Errorf("iteration %d: %w", it, err)
		}
		n := next + acked // ops 0..n-1 acked
		recovered, err := verify(*dir, n)
		if err != nil {
			return fmt.Errorf("iteration %d (seed %d): %w", it, *seed, err)
		}
		next = recovered
		totalAcked += acked
	}
	fmt.Printf("crashtest: PASS, %d iterations, %d acknowledged ops, 0 lost\n", *iters, totalAcked)
	return nil
}

// runAndKill starts a writer at op index start, SIGKILLs it after d, and
// returns how many ops it acknowledged.
func runAndKill(exe, dir string, start int, d time.Duration) (int, error) {
	cmd := exec.Command(exe, "crashwriter", "-dir", dir, "-start", strconv.Itoa(start))
	cmd.Stderr = os.Stderr
	out, err := cmd.StdoutPipe()
	if err != nil {
		return 0, err
	}
	if err := cmd.Start(); err != nil {
		return 0, err
	}
	acked := 0
	done := make(chan error, 1)
	go func() {
		sc := bufio.NewScanner(out)
		for sc.Scan() {
			f := strings.Fields(sc.Text())
			if len(f) != 2 || f[0] != "ack" {
				continue
			}
			if i, err := strconv.Atoi(f[1]); err == nil {
				acked = i - start + 1
			}
		}
		done <- nil
	}()
	time.Sleep(d)
	cmd.Process.Kill()
	<-done // reader hits EOF once the child is dead
	cmd.Wait()
	return acked, nil
}

// verify reopens the database and checks it equals the model after n or n+1
// ops. It returns which one matched.
func verify(dir string, n int) (int, error) {
	d, err := db.Open(dir, crashOptions())
	if err != nil {
		return 0, fmt.Errorf("reopen failed: %w", err)
	}
	defer d.Close()
	got := map[string][]byte{}
	if err := d.Scan(nil, nil, func(k, v []byte) bool {
		got[string(k)] = append([]byte(nil), v...)
		return true
	}); err != nil {
		return 0, err
	}
	model := map[string][]byte{}
	for i := 0; i < n; i++ {
		applyOp(model, i)
	}
	if equal(got, model) {
		return n, nil
	}
	applyOp(model, n)
	if equal(got, model) {
		return n + 1, nil
	}
	model = map[string][]byte{}
	for i := 0; i < n; i++ {
		applyOp(model, i)
	}
	for k, v := range model {
		if g, ok := got[k]; !ok {
			return 0, fmt.Errorf("lost acknowledged key %s (acked ops: %d)", k, n)
		} else if !bytes.Equal(g, v) {
			return 0, fmt.Errorf("key %s has wrong value (acked ops: %d)", k, n)
		}
	}
	return 0, fmt.Errorf("recovered state has unexpected extra keys (acked ops: %d, got %d keys)", n, len(got))
}

func equal(a, b map[string][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if w, ok := b[k]; !ok || !bytes.Equal(v, w) {
			return false
		}
	}
	return true
}
