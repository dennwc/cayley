package ouch

import (
	"context"
	"fmt"
	"math"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cayleygraph/cayley"
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/nosql"
	"github.com/cayleygraph/cayley/graph/nosql/nosqltest"
	"github.com/cayleygraph/cayley/quad"
	"github.com/cayleygraph/cayley/writer"
)

func (db *DB) dcheck(key nosql.Key, d nosql.Document) error {
	col := string(d[collectionField].(nosql.String))
	decoded, err := db.FindByKey(col, key)

	if err != nil {
		return fmt.Errorf("unable to find record to check FindByKey '%v' error: %v", key, err)
	}

	return dcompare(decoded, d)
}

func dcompare(decoded, d nosql.Document) error {
	for k, v := range decoded {
		if k[0] != '_' { // _ fields should have changed
			switch x := v.(type) {
			case nosql.Document:
				if err := dcompare(d[k].(nosql.Document), x); err != nil {
					return err
				}
			case nosql.Key:
				for i, kv := range x {
					if d[k].(nosql.Key)[i] != kv {
						return fmt.Errorf("decoded key %s not-equal original %v", k)
					}
				}
			case nosql.Bytes:
				for i, kv := range x {
					if d[k].(nosql.Bytes)[i] != kv {
						return fmt.Errorf("bytes %s not-equal", k)
					}
				}
			case nosql.Array:
				_, ok := d[k].(nosql.Array)
				if !ok {
					return fmt.Errorf("decoded array not-equal original %s %v %T %v %T", k, v, v, d[k], d[k])
				}
				// check contents of arrays
				for i, kv := range x {
					if err := dcompare(
						nosql.Document{k: d[k].(nosql.Array)[i]},
						nosql.Document{k: kv},
					); err != nil {
						return err
					}
				}
			default:
				if d[k] != v {
					return fmt.Errorf("decoded value not-equal original %s %v %T %v %T", k, v, v, d[k], d[k])
				}
			}
		}
	}
	for k, v := range d {
		if k[0] != '_' { // _ fields should have changed
			switch x := v.(type) {
			case nosql.Document:
				if err := dcompare(x, decoded[k].(nosql.Document)); err != nil {
					return err
				}
			case nosql.Bytes:
				for i, kv := range x {
					if decoded[k].(nosql.Bytes)[i] != kv {
						return fmt.Errorf("bytes %s not-equal", k)
					}
				}
			case nosql.Key:
				for i, kv := range x {
					if decoded[k].(nosql.Key)[i] != kv {
						return fmt.Errorf("keys %s not-equal", k)
					}
				}
			case nosql.Array: // TODO
				_, ok := decoded[k].(nosql.Array)
				if !ok {
					return fmt.Errorf("array not-equal %s %v %T %v %T", k, v, v, decoded[k], decoded[k])
				}
				for i, kv := range x {
					if err := dcompare(
						nosql.Document{k: decoded[k].(nosql.Array)[i]},
						nosql.Document{k: kv},
					); err != nil {
						return err
					}
				}
			default:
				if decoded[k] != v {
					return fmt.Errorf("value not-equal %s %v %T %v %T", k, v, v, decoded[k], decoded[k])
				}
			}
		}
	}
	return nil
}

func testDB() string {
	switch defaultDriverName {
	case "pouch":
		return "pouchdb.test"
	case "couch":
		return "http://127.0.0.1:5984/ouchtest"
	default:
		panic("bad defaultDriverName: " + defaultDriverName)
	}
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

// TestMemstore does those tests possible using the memory backend only (very simple only)
func TestMemstore(t *testing.T) {
	dbc, err := Open(testDB(), graph.Options{})
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
	if err := dbc.Delete("test").Keys(key).Do(context.TODO()); err != nil {
		t.Error(err)
	}
	if doc, err := dbc.(*DB).FindByKey("test", key); err != nosql.ErrNotFound {
		t.Errorf("record not deleted - error: %v doc: %v", err, doc)
	}
	err = dbc.Close()
	if err != nil {
		t.Error("DB close error", err)
	}
}

func TestIntStr(t *testing.T) {
	testI := []int64{120000, -4, 88, 0, -7000000, 88, math.MaxInt64, math.MinInt64}
	testS := []string{}
	for _, v := range testI {
		testS = append(testS, itos(v))
	}
	//t.Log(testS)
	sort.Strings(testS)
	//t.Log(testS)
	sort.Slice(testI, func(i, j int) bool { return testI[i] < testI[j] })
	for k, v := range testS {
		r := stoi(v)
		//t.Logf("Sort of stringed int64: %v %v %v", k, r, testI[k])
		if r != testI[k] {
			t.Errorf("Sorting of stringed int64s wrong: %v %v %v", k, r, testI[k])
		}
	}
}

func cTestHelloWorld(t *testing.T) {

	trace = true
	defer func() { trace = false }()

	store, err := cayley.NewGraph("ouch", testDB(), graph.Options{})
	if err != nil {
		t.Error(err)
		return
	}

	err = store.AddQuad(quad.Make("phrase of the day", "is of course", "Hello World!", nil))
	if err != nil {
		t.Error(err)
		return
	}

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
		t.Error(err)
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

// ALL TESTS...

func makeOuch(t testing.TB) (nosql.Database, graph.Options, func()) {
	qs, err := dialDB(true, testDB(), nil)
	if err != nil {
		t.Fatal(err)
	}
	return qs, nil, func() {
		qs.Close()
	}
}

func xTestOuchAll(t *testing.T) {
	nosqltest.TestAll(t, makeOuch, &nosqltest.Config{
		TimeInMs: true,
	})
}
