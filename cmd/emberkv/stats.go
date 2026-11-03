package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"

	"github.com/Striv11/emberkv/db"
	"github.com/Striv11/emberkv/metrics"
)

// runStats opens a database and prints its metrics in Prometheus text format,
// or with -listen serves them at /metrics until interrupted.
func runStats(args []string) error {
	fs := flag.NewFlagSet("stats", flag.ExitOnError)
	dir := fs.String("dir", "", "database directory (required)")
	listen := fs.String("listen", "", "serve /metrics on this address (e.g. 127.0.0.1:9100)")
	fs.Parse(args)
	if *dir == "" {
		return fmt.Errorf("stats: -dir is required")
	}
	d, err := db.Open(*dir, db.Options{Background: true})
	if err != nil {
		return err
	}
	defer d.Close()
	if *listen == "" {
		return metrics.Write(os.Stdout, d.Stats())
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", metrics.Handler(d))
	return http.ListenAndServe(*listen, mux)
}
