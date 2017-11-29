package ouch

import (
	"fmt"
	"log"
	"testing"
	"time"

	_ "github.com/go-kivik/memorydb"
	"github.com/stretchr/testify/require"

	"github.com/cayleygraph/cayley"
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/nosql"
	"github.com/cayleygraph/cayley/graph/nosql/nosqltest"
	"github.com/cayleygraph/cayley/quad"
	"github.com/cayleygraph/cayley/writer"
)

func testDB() string {
	switch defaultDriverName {
	case "memory":
		return "ouchtest"
	default:
		panic("bad defaultDriverName: " + defaultDriverName)
	}
}

func makeOuch(t testing.TB) (nosql.Database, graph.Options, func()) {
	qs, err := dialDB(true, testDB(), graph.Options{
		"driver": "memory",
	})
	if err != nil {
		t.Fatal(err)
	}
	return qs, nil, func() {
		qs.Close()
	}
}

func xTestOuchAll(t *testing.T) {
	defaultDriverName = "memory" // for initial testing only
	nosqltest.TestAll(t, makeOuch, &nosqltest.Config{
		TimeInMs: true,
	})
}

var allsorts = nosql.Document{
	"Vnil": nil,
	"VDocument": nosql.Document{
		"Velement": nosql.String("test"),
	},
	"VArray":  nosql.Array{nosql.String("A"), nosql.String("B"), nosql.String("C")},
	"VKey":    nosql.Key{"1", "2", "3"},
	"VString": nosql.String("TEST"),
	"VInt":    nosql.Int(42),
	"VFloat":  nosql.Float(42.42),
	"VBool":   nosql.Bool(true),
	"VTime":   nosql.Time(time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC)),
	"VBytes":  nosql.Bytes{1, 2, 3, 4},
}

// TestMemstore does those tests possible using the memory backend (simple only)
func TestMemstore(t *testing.T) {
	defaultDriverName = "memory" // for testing only
	dbc, err := Create(testDB(), graph.Options{})
	if err != nil {
		t.Error("DB create error", defaultDriverName, dbc, err)
		return
	}
	key, err := dbc.Insert("test", nil, allsorts)
	if err != nil {
		t.Error("insert error", err)
	}
	if err := dbc.(*DB).dcheck(key, allsorts); err != nil {
		t.Error(err)
	}
	err = dbc.Close()
	if err != nil {
		t.Error("DB close error", err)
	}
}

func xTestHelloWorld(t *testing.T) {

	defaultDriverName = "memory" // for initial testing only

	store, err := cayley.NewGraph("ouch", testDB(), graph.Options{})
	if err != nil {
		panic(err)
	}

	store.AddQuad(quad.Make("phrase of the day", "is of course", "Hello World!", nil))

	// Now we create the path, to get to our data
	p := cayley.StartPath(store, quad.String("phrase of the day")).Out(quad.String("is of course"))

	// Now we iterate over results. Arguments:
	// 1. Optional context used for cancellation.
	// 2. Flag to optimize query before execution.
	// 3. Quad store, but we can omit it because we have already built path with it.
	err = p.Iterate(nil).EachValue(nil, func(value quad.Value) {
		nativeValue := quad.NativeOf(value) // this converts RDF values to normal Go types
		fmt.Println(nativeValue)
	})
	if err != nil {
		log.Fatalln(err)
	}

}

// below from the memstore tests

// This is a simple test graph.
//
//    +---+                        +---+
//    | A |-------               ->| F |<--
//    +---+       \------>+---+-/  +---+   \--+---+
//                 ------>|#B#|      |        | E |
//    +---+-------/      >+---+      |        +---+
//    | C |             /            v
//    +---+           -/           +---+
//      ----    +---+/             |#G#|
//          \-->|#D#|------------->+---+
//              +---+
//
var simpleGraph = []quad.Quad{
	quad.MakeRaw("A", "follows", "B", ""),
	quad.MakeRaw("C", "follows", "B", ""),
	quad.MakeRaw("C", "follows", "D", ""),
	quad.MakeRaw("D", "follows", "B", ""),
	quad.MakeRaw("B", "follows", "F", ""),
	quad.MakeRaw("F", "follows", "G", ""),
	quad.MakeRaw("D", "follows", "G", ""),
	quad.MakeRaw("E", "follows", "F", ""),
	quad.MakeRaw("B", "status", "cool", "status_graph"),
	quad.MakeRaw("D", "status", "cool", "status_graph"),
	quad.MakeRaw("G", "status", "cool", "status_graph"),
}

func makeTestStore(data []quad.Quad) (graph.QuadStore, graph.QuadWriter, []pair) {
	defaultDriverName = "memory" // for initial testing only

	seen := make(map[string]struct{})
	qs, err := graph.NewQuadStore("ouch", testDB(), graph.Options{})
	if err != nil {
		panic(err)
	}
	var (
		val int64
		ind []pair
	)
	writer, _ := writer.NewSingleReplication(qs, nil)
	for _, t := range data {
		for _, dir := range quad.Directions {
			qp := t.GetString(dir)
			if _, ok := seen[qp]; !ok && qp != "" {
				val++
				ind = append(ind, pair{qp, val})
				seen[qp] = struct{}{}
			}
		}

		writer.AddQuad(t)
		val++
	}
	return qs, writer, ind
}

type pair struct {
	query string
	value int64
}

func xTestSimpleGraph(t *testing.T) {
	qs, _, index := makeTestStore(simpleGraph)

	t.Log(qs, index)

	require.Equal(t, int64(22), qs.Size())

	// TODO replicate
	// for _, test := range index {
	// 	v := qs.ValueOf(quad.Raw(test.query))
	// 	switch v := v.(type) {
	// 	default:
	// 		t.Errorf("ValueOf(%q) returned unexpected type, got:%T expected int64", test.query, v)
	// 	case bnode:
	// 		require.Equal(t, test.value, int64(v))
	// 	}
	// }
}
