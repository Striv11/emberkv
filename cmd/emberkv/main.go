// Command emberkv runs the emberkv benchmarks and the crash-recovery test.
//
//	emberkv bench     [-n 100000] [-value 100] [-sync=false] [-dir DIR]
//	emberkv crashtest [-iters 100] [-seed N] [-dir DIR]
package main

import (
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "bench":
		err = runBench(os.Args[2:])
	case "crashtest":
		err = runCrashTest(os.Args[2:])
	case "crashwriter": // internal: the child process killed by crashtest
		err = runCrashWriter(os.Args[2:])
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "emberkv:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: emberkv bench|crashtest [flags]")
}
