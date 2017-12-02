package ouch

import (
	"context"
	"fmt"
	"math"
	"runtime"
	"sort"
	"testing"
	"time"

	"github.com/cayleygraph/cayley"
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/nosql"
	"github.com/cayleygraph/cayley/graph/nosql/nosqltest"
	"github.com/cayleygraph/cayley/quad"
	"github.com/flimzy/kivik"
)

func testDB(DBshouldNotExist bool) ([]string, error) {
	var ret []string
	const remote = "http://testusername:testpassword@127.0.0.1:5984/ouchtest"
	switch defaultDriverName {
	case "pouch":
		ret = []string{"pouchdb.test", remote} // test both local and remote db access
	case "couch":
		ret = []string{remote}
	default:
		panic("bad defaultDriverName: " + defaultDriverName)
	}
	for _, r := range ret {
		if err := deleteAllOuchDocs(r, DBshouldNotExist); err != nil {
			fmt.Println("DEBUG delete test db", r, "error: ", err)
		}
	}
	return ret, nil
}

func deleteAllOuchDocs(testDBname string, DBshouldNotExist bool) error {
	if DBshouldNotExist {
		client, err := kivik.New(context.TODO(), defaultDriverName, testDBname)
		if err != nil {
			return err
		}
		return client.DestroyDB(context.TODO(), testDBname)
	}

	db, err := Open(testDBname, graph.Options{})
	if err != nil {
		fmt.Printf("DEBUG deleteAllOuchDocs() failed open error:", err)
		_, err = Create(testDBname, nil)
		return err
	}
	defer db.Close()

	rows, err := db.(*DB).db.AllDocs(context.TODO())
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		//fmt.Println("deleteAllOuchDocs", rows.ID())
		err = db.Delete("").Keys(nosql.Key{rows.ID()}).Do(context.TODO())
		if err != nil {
			return err
		}
	}
	return nil
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

// TestInsertDelete does very basic testing
func TestInsertDelete(t *testing.T) {
	// trace = true
	// defer func() { trace = false }()

	dbNames, err := testDB(false)
	if err != nil {
		t.Error("DB setup error", err)
		return
	}

	for _, dbName := range dbNames {

		//fmt.Println("DEBUG dbName:", dbName)

		dbc, err := Open(dbName, graph.Options{})
		if err != nil {
			t.Error("DB open error", defaultDriverName, dbName, err)
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
			t.Errorf("record not deleted - error: %v key: %v doc: %v", err, key, doc)
		}
		err = dbc.Close()
		if err != nil {
			t.Error("DB close error", err)
		}
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

func TestHelloWorld(t *testing.T) {

	dbNames, err := testDB(false)
	if err != nil {
		t.Error("DB setup error", err)
		return
	}

	// trace = true
	// defer func() { trace = false }()

	for _, dbName := range dbNames {

		store, err := cayley.NewGraph("ouch", dbName, graph.Options{})
		if err != nil {
			t.Error(err)
			return
		}

		const helloWorld = "Hello World!"
		err = store.AddQuad(quad.Make("phrase of the day", "is of course", helloWorld, nil))
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
			// fmt.Printf("HW %T %v\n", value, value)
			if value != quad.String(helloWorld) {
				t.Errorf("NOT " + helloWorld)
			}
		})
		if err != nil {
			t.Error(err)
		}
	}
}

// ALL TESTS...

func TestOuchAll(t *testing.T) {

	if runtime.GOARCH == "js" {
		t.Skip("JS/PouchDB not yet ready for primetime")
	}

	// trace = true
	// defer func() { trace = false }()

	dbNames, err := testDB(true)
	if err != nil {
		t.Fatal(err)
	}
	for _, dbName := range dbNames {
		t.Log("Testing against:", dbName)
		makeOuch := func(t testing.TB) (nosql.Database, graph.Options, func()) {
			deleteAllOuchDocs(dbName, true)
			qs, err := dialDB(true, dbName, nil)
			if err != nil {
				t.Fatal(err)
			}
			return qs, nil, func() {
				qs.Close()
			}
		}
		nosqltest.TestAll(t, makeOuch, &nosqltest.Config{
			TimeInMs: true,
		})
	}
}
