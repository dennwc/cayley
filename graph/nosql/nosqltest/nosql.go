package nosqltest

import (
	"context"
	"fmt"
	"sort"
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

func (kt keyType) SetKey(d nosql.Document, k nosql.Key) {
	for i, f := range kt.Fields {
		d[f] = nosql.String(k[i])
	}
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
	{name: "insert", t: testInsert},
	{name: "delete by key", t: testDeleteByKey},
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

func newDoc(d nosql.Document) nosql.Document {
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
	// TODO: time type
	return d
}

func fixDoc(conf *Config, d nosql.Document) {
	if conf.FloatToInt {
		for k, v := range d {
			if f, ok := v.(nosql.Float); ok && nosql.Float(nosql.Int(f)) == f {
				d[k] = nosql.Int(f)
			}
		}
	}
}

type byFields []string

func (s byFields) Key(d nosql.Document) nosql.Key {
	return nosql.KeyFrom(s, d)
}
func (s byFields) Less(d1, d2 nosql.Document) bool {
	k1, k2 := s.Key(d1), s.Key(d2)
	for i := range k1 {
		s1, s2 := k1[i], k2[i]
		if s1 < s2 {
			return true
		}
	}
	return false
}

type docsAndKeys struct {
	LessFunc func(d1, d2 nosql.Document) bool
	Docs     []nosql.Document
	Keys     []nosql.Key
}

func (s docsAndKeys) Len() int {
	return len(s.Docs)
}

func (s docsAndKeys) Less(i, j int) bool {
	return s.LessFunc(s.Docs[i], s.Docs[j])
}

func (s docsAndKeys) Swap(i, j int) {
	s.Docs[i], s.Docs[j] = s.Docs[j], s.Docs[i]
	s.Keys[i], s.Keys[j] = s.Keys[j], s.Keys[i]
}

func iterateExpect(t testing.TB, kt keyType, it nosql.DocIterator, exp []nosql.Document) {
	ctx := context.TODO()
	defer it.Close()
	var (
		got  []nosql.Document
		keys []nosql.Key
	)
	for it.Next(ctx) {
		keys = append(keys, it.Key())
		got = append(got, it.Doc())
	}
	require.NoError(t, it.Err())

	sorter := byFields(kt.Fields)
	exp = append([]nosql.Document{}, exp...)
	sort.Slice(exp, func(i, j int) bool {
		return sorter.Less(exp[i], exp[j])
	})
	var expKeys []nosql.Key
	for _, d := range exp {
		expKeys = append(expKeys, sorter.Key(d))
	}

	sort.Sort(docsAndKeys{
		LessFunc: sorter.Less,
		Docs:     got, Keys: keys,
	})
	require.Equal(t, exp, got)
	require.Equal(t, expKeys, keys)
}

func testInsert(t *testing.T, db nosql.Database, conf *Config, kt keyType) {
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

	var docs []nosql.Document
	for _, in := range ins {
		doc, err := db.FindByKey(col, in.Key)
		require.NoError(t, err, "find %#v", in.Key)
		kt.SetKey(in.Doc, in.Key)
		fixDoc(conf, in.Doc)
		require.Equal(t, in.Doc, doc, "got: %#v", doc)
		docs = append(docs, in.Doc)
	}

	_, err = db.FindByKey(col, kt.Gen())
	require.Equal(t, nosql.ErrNotFound, err)

	iterateExpect(t, kt, db.Query(col).Iterate(), docs)
}

func testDeleteByKey(t *testing.T, db nosql.Database, conf *Config, kt keyType) {
	const (
		col = "test"
	)
	err := db.EnsureIndex(col, nosql.Index{
		Fields: kt.Fields,
		Type:   nosql.StringExact,
	}, nil)
	require.NoError(t, err)

	var (
		keys []nosql.Key
		docs []nosql.Document
	)
	for i := 0; i < 10; i++ {
		var (
			key nosql.Key
			err error
		)
		doc := nosql.Document{
			"data": nosql.Int(i),
		}
		if len(kt.Fields) == 1 && i%2 == 1 {
			key, err = db.Insert(col, nil, doc)
		} else {
			key, err = db.Insert(col, kt.Gen(), doc)
		}
		require.NoError(t, err)
		keys = append(keys, key)

		kt.SetKey(doc, key)
		fixDoc(conf, doc)
		docs = append(docs, doc)
	}

	iterateExpect(t, kt, db.Query(col).Iterate(), docs)

	del := keys[:5]
	keys = keys[len(del):]
	docs = docs[len(del):]

	err = db.Delete(col).Keys(del[0]).Do(context.TODO())
	require.NoError(t, err)

	err = db.Delete(col).Keys(del[1:]...).Do(context.TODO())
	require.NoError(t, err)

	iterateExpect(t, kt, db.Query(col).Iterate(), docs)
}
