package mongo

import (
	"github.com/google/cayley/graph"
	"github.com/google/cayley/graph/graphtest"
	"github.com/google/cayley/internal/dock"
	"testing"
)

func makeMongo(t testing.TB) (graph.QuadStore, func()) {
	var conf dock.Config

	conf.Image = "mongo:3"
	conf.OpenStdin = true
	conf.Tty = true

	addr, closer := dock.Run(t, conf)
	addr = addr + ":27017"
	if err := createNewMongoGraph(addr, nil); err != nil {
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

func TestMongoAll(t *testing.T) {
	graphtest.TestAll(t, makeMongo, &graphtest.Config{
		SkipDeletedFromIterator:  true,
		SkipSizeCheckAfterDelete: true,
	})
}
