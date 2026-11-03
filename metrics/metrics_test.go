package metrics

import (
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Striv11/emberkv/db"
)

func TestHandlerExposition(t *testing.T) {
	d, err := db.Open(t.TempDir(), db.Options{NoSync: true})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	d.Put([]byte("a"), []byte("1"))
	d.Put([]byte("b"), []byte("2"))
	d.Delete([]byte("a"))
	d.Get([]byte("b"))

	srv := httptest.NewServer(Handler(d))
	defer srv.Close()
	resp, err := srv.Client().Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Fatalf("content type %q", ct)
	}
	b, _ := io.ReadAll(resp.Body)
	body := string(b)
	for _, want := range []string{
		"# TYPE emberkv_puts_total counter\nemberkv_puts_total 2\n",
		"emberkv_deletes_total 1\n",
		"emberkv_gets_total 1\n",
		"emberkv_last_sequence 3\n",
		"# TYPE emberkv_level_tables gauge\n",
		"emberkv_level_tables{level=\"0\"} 0\n",
		"emberkv_level_bytes{level=\"6\"} 0\n",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("missing %q in:\n%s", want, body)
		}
	}
	// Every sample line must be "name[{labels}] value" (no stray text).
	for _, line := range strings.Split(strings.TrimSpace(body), "\n") {
		if strings.HasPrefix(line, "#") {
			continue
		}
		if len(strings.Fields(line)) != 2 {
			t.Errorf("malformed sample line %q", line)
		}
	}
}
