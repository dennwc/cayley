package nosqltest

import (
	"fmt"
	"sync"
	"testing"

	"github.com/cayleygraph/cayley/graph/nosql"
	"github.com/stretchr/testify/require"
)

type keyType struct {
	Name   string
	Fields []string
	Gen    func() nosql.Key
}

var (
	mu      sync.Mutex
	lastKey int
)

func next() int {
	mu.Lock()
	lastKey++
	v := lastKey
	mu.Unlock()
	return v
}

var keyTypes = []keyType{
	{
		Name:   "single",
		Fields: []string{"id"},
		Gen: func() nosql.Key {
			v := next()
			return nosql.Key{fmt.Sprintf("k%d", v)}
		},
	},
	{
		Name:   "composite2",
		Fields: []string{"id1", "id2"},
		Gen: func() nosql.Key {
			v := next()
			return nosql.Key{
				fmt.Sprintf("i%d", v),
				fmt.Sprintf("j%d", v),
			}
		},
	},
	{
		Name:   "composite3",
		Fields: []string{"id1", "id2", "id3"},
		Gen: func() nosql.Key {
			v := next()
			return nosql.Key{
				fmt.Sprintf("i%d", v),
				fmt.Sprintf("j%d", v),
				fmt.Sprintf("k%d", v),
			}
		},
	},
}

var testsNoSQLKey = []struct {
	name string
	t    func(t *testing.T, db nosql.Database, conf *Config, kt keyType)
}{
	{name: "insert and find", t: testInsertAndFind},
}

func TestNoSQL(t *testing.T, gen DatabaseFunc, conf *Config) {
	for _, kt := range keyTypes {
		t.Run(kt.Name, func(t *testing.T) {
			for _, c := range testsNoSQLKey {
				t.Run(c.name, func(t *testing.T) {
					db, _, closer := gen(t)
					defer closer()

					c.t(t, db, conf, kt)
				})
			}
		})
	}
}

func testInsertAndFind(t *testing.T, db nosql.Database, conf *Config, kt keyType) {
	const (
		col = "test"
	)
	err := db.EnsureIndex(col, nosql.Index{
		Fields: kt.Fields,
		Type:   nosql.StringExact,
	}, nil)
	require.NoError(t, err)

	_, err = db.FindByKey(col, kt.Gen())
	require.Equal(t, nosql.ErrNotFound, err)

	newDoc := func(d nosql.Document) nosql.Document {
		d["val_key"] = nosql.Strings{"a"}
		d["val_key2"] = nosql.Strings{"a", "b"}
		d["val_str"] = nosql.String("bar")
		d["val_int"] = nosql.Int(42)
		d["val_int0"] = nosql.Int(0)
		d["val_float"] = nosql.Float(42.3)
		d["val_floati"] = nosql.Float(42)
		d["val_float0"] = nosql.Float(0)
		d["val_bool"] = nosql.Bool(true)
		d["val_boolf"] = nosql.Bool(false)
		// TODO: test time type
		return d
	}

	type insert struct {
		Key nosql.Key
		Doc nosql.Document
	}

	k1 := kt.Gen()
	doc1 := make(nosql.Document)
	for i, f := range kt.Fields {
		doc1[f] = nosql.String(k1[i])
	}
	k2 := kt.Gen()
	ins := []insert{
		{ // set key in doc and in insert
			Key: k1,
			Doc: newDoc(doc1),
		},
		{ // set key on insert, but not in doc
			Key: k2,
			Doc: newDoc(nosql.Document{}),
		},
	}
	if len(kt.Fields) == 1 {
		ins = append(ins, insert{
			// auto-generate key
			Doc: newDoc(nosql.Document{}),
		})
	}
	for i := range ins {
		in := &ins[i]
		k, err := db.Insert(col, in.Key, in.Doc)
		require.NoError(t, err)
		if in.Key == nil {
			require.NotNil(t, k)
			in.Key = k
		} else {
			require.Equal(t, in.Key, k)
		}
	}

	for _, in := range ins {
		doc, err := db.FindByKey(col, in.Key)
		require.NoError(t, err, "find %#v", in.Key)
		for i, f := range kt.Fields {
			if _, ok := in.Doc[f]; !ok {
				in.Doc[f] = nosql.String(in.Key[i])
			}
		}
		if conf.FloatToInt {
			delete(in.Doc, "val_floati")
			delete(in.Doc, "val_float0")
			in.Doc["val_floati"] = nosql.Int(42)
			in.Doc["val_float0"] = nosql.Int(0)
		}
		require.Equal(t, in.Doc, doc, "got: %#v", doc)
	}

	_, err = db.FindByKey(col, kt.Gen())
	require.Equal(t, nosql.ErrNotFound, err)
}
