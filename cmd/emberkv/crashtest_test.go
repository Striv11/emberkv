package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func buildCLI(t *testing.T) string {
	t.Helper()
	exe := filepath.Join(t.TempDir(), "emberkv")
	if out, err := exec.Command("go", "build", "-o", exe, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	return exe
}

func TestCrashTestPasses(t *testing.T) {
	exe := buildCLI(t)
	out, err := exec.Command(exe, "crashtest", "-iters", "8", "-seed", "42").CombinedOutput()
	if err != nil {
		t.Fatalf("crashtest failed: %v\n%s", err, out)
	}
}

func TestBenchRuns(t *testing.T) {
	exe := buildCLI(t)
	if out, err := exec.Command(exe, "bench", "-n", "2000").CombinedOutput(); err != nil {
		t.Fatalf("bench failed: %v\n%s", err, out)
	}
}

// The verifier must notice lost acknowledged data.
func TestVerifyDetectsLoss(t *testing.T) {
	dir := t.TempDir()
	if n, err := verify(dir, 0); err != nil || n != 0 {
		t.Fatalf("empty: n=%d err=%v", n, err)
	}
	if _, err := verify(dir, 5); err == nil {
		t.Fatal("verify accepted a database missing acknowledged ops")
	}
	_ = os.RemoveAll(dir)
}
