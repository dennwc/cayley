package bolt

import (
	"github.com/boltdb/bolt"
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/graphtest"
	"io/ioutil"
	"os"
	"testing"
	"time"
)

func makeBoltDB(t testing.TB) (*bolt.DB, func()) {
	tmpFile, err := ioutil.TempFile(os.TempDir(), "cayley_test")
	if err != nil {
		t.Fatalf("Could not create working directory: %v", err)
	}
	s, err := bolt.Open(tmpFile.Name(), 0755, &bolt.Options{Timeout: time.Minute})
	if err != nil {
		os.RemoveAll(tmpFile.Name())
		t.Fatal("Failed to create Bolt database.", err)
	}
	return s, func() {
		s.Close()
		os.RemoveAll(tmpFile.Name())
	}
}

func makeBolt(t testing.TB) (graph.QuadStore, graph.Options, func()) {
	tmpFile, err := ioutil.TempFile(os.TempDir(), "cayley_test")
	if err != nil {
		t.Fatalf("Could not create working directory: %v", err)
	}
	s, err := Create(tmpFile.Name(), nil)
	if err != nil {
		os.RemoveAll(tmpFile.Name())
		t.Fatal("Failed to create Bolt database.", err)
	}
	qs := NewQuadStore(s)
	return qs, nil, func() {
		qs.Close()
		s.Close()
		os.RemoveAll(tmpFile.Name())
	}
}

func TestBoltAll(t *testing.T) {
	graphtest.TestAll(t, makeBolt, &graphtest.Config{
		SkipNodeDelAfterQuadDel: true,
	})
}
