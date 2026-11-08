// Package metrics renders db.Stats in the Prometheus text exposition format
// and serves it over HTTP. It only reads local state; nothing is sent anywhere.
package metrics

import (
	"fmt"
	"io"
	"net/http"

	"github.com/Striv11/emberkv/db"
)

// Write renders s to w in Prometheus text format (version 0.0.4).
func Write(w io.Writer, s db.Stats) error {
	var err error
	emit := func(name, typ, help string, v any) {
		if err == nil {
			_, err = fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n%s %v\n", name, help, name, typ, name, v)
		}
	}
	emit("emberkv_puts_total", "counter", "Put operations since open.", s.Puts)
	emit("emberkv_deletes_total", "counter", "Delete operations since open.", s.Deletes)
	emit("emberkv_gets_total", "counter", "Get operations since open.", s.Gets)
	emit("emberkv_scans_total", "counter", "Scan operations since open.", s.Scans)
	emit("emberkv_flushes_total", "counter", "Memtable flushes since open.", s.Flushes)
	emit("emberkv_compactions_total", "counter", "Compactions since open.", s.Compactions)
	emit("emberkv_background_retries_total", "counter", "Background flush/compaction builds retried after an I/O error.", s.BGRetries)
	emit("emberkv_memtable_bytes", "gauge", "Approximate memtable size.", s.MemtableBytes)
	emit("emberkv_last_sequence", "gauge", "Highest assigned sequence number.", s.LastSeq)
	emit("emberkv_block_cache_hits_total", "counter", "Block cache hits.", s.Cache.Hits)
	emit("emberkv_block_cache_misses_total", "counter", "Block cache misses.", s.Cache.Misses)
	emit("emberkv_block_cache_evictions_total", "counter", "Block cache evictions.", s.Cache.Evictions)
	emit("emberkv_block_cache_bytes", "gauge", "Bytes held by the block cache.", s.Cache.Bytes)
	if err != nil {
		return err
	}
	for _, g := range []struct{ name, help string }{
		{"emberkv_level_tables", "Live SSTables per level."},
		{"emberkv_level_bytes", "Bytes of live SSTables per level."},
	} {
		if _, err = fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s gauge\n", g.name, g.help, g.name); err != nil {
			return err
		}
		for l, ls := range s.Levels {
			v := uint64(ls.Tables)
			if g.name == "emberkv_level_bytes" {
				v = ls.Bytes
			}
			if _, err = fmt.Fprintf(w, "%s{level=\"%d\"} %d\n", g.name, l, v); err != nil {
				return err
			}
		}
	}
	return nil
}

// Handler serves the metrics of d.
func Handler(d *db.DB) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		Write(w, d.Stats())
	})
}
