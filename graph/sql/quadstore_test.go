package sql

import (
	"github.com/google/cayley/graph"
	"github.com/google/cayley/graph/graphtest"
	"github.com/google/cayley/internal/dock"
	"github.com/lib/pq"
	"testing"
)

func makePostgres(t testing.TB) (graph.QuadStore, func()) {
	var conf dock.Config

	conf.Image = "postgres:9"
	conf.OpenStdin = true
	conf.Tty = true
	conf.Env = []string{`POSTGRES_PASSWORD=postgres`}

	addr, closer := dock.RunAndWait(t, conf, func(addr string) bool {
		conn, err := pq.Open(`postgres://postgres:postgres@` + addr + `/postgres?sslmode=disable`)
		if err != nil {
			return false
		}
		conn.Close()
		return true
	})
	addr = `postgres://postgres:postgres@` + addr + `/postgres?sslmode=disable`
	if err := createSQLTables(addr, nil); err != nil {
		closer()
		t.Fatal(err)
	}
	qs, err := newQuadStore(addr, nil)
	if err != nil {
		closer()
		t.Fatal(err)
	}
	return qs, func() {
		qs.Close()
		closer()
	}
}

func TestPostgresAll(t *testing.T) {
	graphtest.TestAll(t, makePostgres, &graphtest.Config{
		SkipSizeCheckAfterDelete: true,
		UnTyped:                  true,
	})
}
