package ouch

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/nosql"

	"github.com/flimzy/kivik"
)

const Type = "ouch"

func init() {
	nosql.Register(Type, nosql.Registration{
		NewFunc:      Open,
		InitFunc:     Create,
		IsPersistent: true,
	})
}

func dialDB(create bool, addr string, opt graph.Options) (*DB, error) {
	dpanic("dialDB")
	ctx := context.TODO() // TODO - replace with parameter value

	driver := defaultDriverName

	//fmt.Println("DEBUG dialDB", driver, create, addr, opt)

	addrParsed, err := url.Parse(addr)
	if err != nil {
		return nil, err
	}

	pathParts := strings.Split(addrParsed.Path, "/")
	dbName := ""
	if len(pathParts) > 0 && addr != "" {
		dbName = pathParts[len(pathParts)-1]
	} else {
		return nil, errors.New("unable to decypher database name from: " + addr)
	}
	dsn := strings.TrimSuffix(addr, dbName)

	client, err := kivik.New(ctx, driver, dsn)
	if err != nil {
		return nil, err
	}

	if create {
		err := client.CreateDB(ctx, dbName)
		if err != nil {
			return nil, err
		}
	}

	db, err := client.DB(ctx, dbName)
	if err != nil {
		return nil, err
	}

	return &DB{
		db:     db,
		colls:  make(map[string]collection),
		driver: driver,
	}, nil
}

func Create(addr string, opt graph.Options) (nosql.Database, error) {
	return dialDB(true, addr, opt)
}

func Open(addr string, opt graph.Options) (nosql.Database, error) {
	return dialDB(false, addr, opt)
}

type collection struct {
	primary   nosql.Index
	secondary []nosql.Index
}

type DB struct {
	db     *kivik.DB
	colls  map[string]collection
	driver string
}

func (db *DB) Close() error {
	// seems no way to close a kivik session, so just let the Go garbage collection do its stuff...
	db.db = nil
	db.colls = nil
	return nil
}

func (db *DB) EnsureIndex(col string, primary nosql.Index, secondary []nosql.Index) error {
	dpanic("DB.EnsureIndex")
	if primary.Type != nosql.StringExact {
		return fmt.Errorf("unsupported type of primary index: %v", primary.Type)
	}
	//fmt.Println("DEBUG TODO DB.EnsureIndex", col, primary, secondary)
	db.colls[col] = collection{
		primary:   primary,
		secondary: secondary,
	}
	// TODO add indexes...
	return nil

}

const idField = "_id"
const revField = "_rev"
const collectionField = "Collection"

func compKey(col string, key nosql.Key) string {
	return "K" + strings.Join(key, "|")
}

func (db *DB) Insert(col string, key nosql.Key, d nosql.Document) (nosql.Key, error) {
	dpanic("DB.Insert")
	k, _, e := db.insert(col, key, d)
	return k, e
}
func (db *DB) insert(col string, key nosql.Key, d nosql.Document) (nosql.Key, string, error) {
	dpanic("DB.insert")
	ctx := context.TODO() // TODO - replace with parameter value

	// fmt.Println("DEBUG INSERT", col, key)
	// fmt.Printf("doc: %#v\n", d)

	if d == nil {
		return nil, "", errors.New("no document to insert")
	}

	rev := ""
	if key == nil {
		key = nosql.GenKey()
	} else {
		var err error
		var id string
		_, id, rev, err = db.findByKey(col, key)
		if err == nil {
			fmt.Println("DEBUG insert over existing record", key, rev)
			rev, err = db.db.Delete(ctx, id, rev) // delete it to be sure it is removed
			if err != nil {
				fmt.Println("DEBUG insert over existing record delete error", err)
				return nil, "", err
			}
		}
	}

	if cP, found := db.colls[col]; found {
		//fmt.Println("DEBUG INSERT KEY DEF", cP)
		// go through the key list an put each in
		if len(cP.primary.Fields) == len(key) {
			for idx, nam := range cP.primary.Fields {
				d[nam] = nosql.String(key[idx]) // just replace with the given key, even if there already
			}
		}
	} else {
		//fmt.Println("DEBUG INSERT NO KEY DEF")
	}

	interfaceDoc := toOuchDoc(col, toOuchValue(idField, key.Value()).(string), rev, d)

	if trace {
		for _, v := range interfaceDoc {
			if str, isStr := v.(string); isStr {
				if strings.Contains(str, findStr) {
					fmt.Printf("DEBUG insert native %#v\n", interfaceDoc)
					break
				}
			}
		}
	}

	_, rev, err := db.db.CreateDoc(ctx, interfaceDoc)
	if err != nil {
		print("DEBUG Insert error " + err.Error())
		return nil, "", err
	}

	//fmt.Println("DEBUG returned Insert Key:", key, interfaceDoc)

	return key, rev, nil
}

func (db *DB) FindByKey(col string, key nosql.Key) (nosql.Document, error) {
	dpanic("DB.FindByKey")

	decoded, _, _, err := db.findByKey(col, key)

	return decoded, err
}

func (db *DB) findByKey(col string, key nosql.Key) (nosql.Document, string, string, error) {
	dpanic("DB.findByKey")
	ctx := context.TODO() // TODO - replace with parameter value

	cK := compKey(col, key)

	row, err := db.db.Get(ctx, cK)
	if err != nil {
		if kivik.StatusCode(err) == kivik.StatusNotFound {
			return nil, "", "", nosql.ErrNotFound
		}
		return nil, "", "", err
	}

	rowDoc := make(map[string]interface{})
	err = row.ScanDoc(&rowDoc)
	if err != nil {
		return nil, "", "", err
	}
	decoded := fromOuchDoc(rowDoc)

	//fmt.Println("DEBUG", col, key, "FOUND", decoded)

	return decoded, rowDoc[idField].(string), rowDoc[revField].(string), nil
}

func (db *DB) Query(col string) nosql.Query {
	dpanic("DB.Query")
	qry := &Query{db: db,
		pathFilters: make(map[string][]nosql.FieldFilter),
		ouchQuery: map[string]interface{}{
			"selector": map[string]interface{}{},
			"limit":    1000000, // million row limit, default is 25
		},
	}
	if col != "" {
		qry.ouchQuery["selector"].(map[string]interface{})[collectionField] = col
	}
	return qry
}
func (db *DB) Update(col string, key nosql.Key) nosql.Update {
	dpanic("DB.Update")
	//fmt.Println("DEBUG ", col, key)
	return &Update{db: db, col: col, key: key, update: nosql.Document{}}
}
func (db *DB) Delete(col string) nosql.Delete {
	dpanic("DB.Delete")

	return &Delete{db: db, col: col, q: db.Query(col)}
}

type ouchQuery map[string]interface{}

type Query struct {
	db          *DB
	pathFilters map[string][]nosql.FieldFilter
	ouchQuery
}

func (q *Query) WithFields(filters ...nosql.FieldFilter) nosql.Query {
	dpanic("Query.WithFields")

	for _, filter := range filters {
		j := strings.Join(filter.Path, keySeparator)
		q.pathFilters[j] = append(q.pathFilters[j], filter)
	}

	return q
}

func (q *Query) buildFilters() nosql.Query {
	dpanic("Query.WithFields")

	for jp, filterList := range q.pathFilters {
		term := map[string]interface{}{}
		for _, filter := range filterList {
			test := ""
			testValue := toOuchValue(".", filter.Value)
			if stringValue, isString := testValue.(string); isString && len(stringValue) > 0 {
				switch filter.Filter {
				case nosql.Equal, nosql.NotEqual:
				// nothing to do as not a relative test
				default:
					typeChar := stringValue[0]
					term["$gte"] = string(typeChar)
					typeCharNext := typeChar + 1
					term["$lt"] = string(typeCharNext)
				}
			}
			switch filter.Filter {
			case nosql.Equal:
				test = "$eq"
			case nosql.NotEqual:
				test = "$ne"
			case nosql.GT:
				test = "$gt"
			case nosql.GTE:
				test = "$gte"
			case nosql.LT:
				test = "$lt"
			case nosql.LTE:
				test = "$lte"
			default:
				msg := "unknown nosqlFilter " + fmt.Sprintf("%v", filter.Filter)
				fmt.Println(msg)
				panic(msg)
			}
			term[test] = testValue
		}
		q.ouchQuery["selector"].(map[string]interface{})[jp] = term
	}

	//fmt.Printf("DEBUG query %#v\n", q)
	return q
}

func (q *Query) Limit(n int) nosql.Query {
	dpanic("Query.Limit")
	q.ouchQuery["limit"] = n
	return q
}

// TODO look for "count" functionality in the interface, rather than counting the rows
func (q *Query) Count(ctx context.Context) (int64, error) {
	dpanic("Query.Count")
	it := q.Iterate()
	//defer it.Close() // closed automatically at the last Next()
	var count int64
	for it.(*Iterator).rows.Next() { // for speed, use the native Next
		count++
	}
	//fmt.Println("DEBUG count", count, it.Err())
	return count, it.(*Iterator).err
}

func (q *Query) One(ctx context.Context) (nosql.Document, error) {
	dpanic("Query.One")
	it := q.Iterate()
	defer it.Close()
	if err := it.Err(); err != nil {
		return nil, err
	}
	if it.Next(ctx) {
		return it.Doc(), it.(*Iterator).err
	}
	return nil, nosql.ErrNotFound
}

func (q *Query) Iterate() nosql.DocIterator {
	dpanic("Query.Iterate")
	ctx := context.TODO() // TODO - replace with parameter value
	q.buildFilters()
	q.debug()
	rows, err := q.db.db.Find(ctx, q.ouchQuery)
	return &Iterator{rows: rows, err: err}
}

type Iterator struct {
	err     error
	rows    *kivik.Rows
	doc     map[string]interface{}
	hadNext bool
	closed  bool
}

func (it *Iterator) Next(ctx context.Context) bool {
	dpanic("Iterator.Next")
	it.hadNext = true
	if it.err != nil || it.closed {
		return false
	}
	it.doc = nil
	haveNext := it.rows.Next()
	it.err = it.rows.Err()
	if it.err != nil {
		return false
	}
	if haveNext {
		it.scanDoc()
		if it.err != nil {
			return false
		}
	} else {
		it.closed = true // auto-closed at end of iteration by API
	}
	return haveNext
}

func (it *Iterator) Err() error {
	dpanic("Iterator.Err")
	if it.err != nil {
		dpanic(it.err.Error())
	}
	return it.err
}

func (it *Iterator) Close() error {
	dpanic("Iterator.Close")
	if it.err == nil && !it.closed {
		it.err = it.rows.Close()
	}
	it.closed = true
	return it.err
}

func (it *Iterator) Key() nosql.Key {
	dpanic("Iterator.Key")
	if it.err != nil || it.closed {
		fmt.Println("DEBUG iterator.Key nil return 1", it.err, it.closed)
		return nil
	}
	if !it.hadNext {
		it.err = errors.New("call to Iterator.Key before Iterator.Next")
		fmt.Println("DEBUG iterator.Key nil return 2", it.err)
		return nil
	}

	var id string
	var haveID bool
	id, haveID = it.doc[idField].(string)
	if haveID {
		//fmt.Printf("DEBUG Iterator.Key fallback method doc[idField] %T %v\n", it.doc[idField], it.doc[idField])
	} else {
		it.err = fmt.Errorf("Iterator.Key ID empty")
		return nil
	}
	ret := nosql.Key([]string(fromOuchValue("?", id).(nosql.Strings)))
	//fmt.Println("DEBUG returned Iterator.Key:", ret)
	return ret
}
func (it *Iterator) Doc() nosql.Document {
	dpanic("Iterator.Doc")
	if it.err != nil || it.closed {
		return nil
	}
	if !it.hadNext {
		it.err = errors.New("Iterator.Doc called before Iterator.Next")
		return nil
	}
	return fromOuchDoc(it.doc)
}

func (it *Iterator) scanDoc() {
	dpanic("Iterator.doc")
	if it.doc == nil && it.err == nil && !it.closed {
		it.doc = map[string]interface{}{}
		it.err = it.rows.ScanDoc(&it.doc)
		if it.err != nil {
			fmt.Printf("DEBUG Iterator.scandoc error %#v\n", it.err)
		}
		if trace {
			fmt.Println("DEBUG scanDoc:", it.doc)
		}
	}
}

type Delete struct {
	db   *DB
	col  string
	q    nosql.Query
	keys []interface{}
}

func (d *Delete) WithFields(filters ...nosql.FieldFilter) nosql.Delete {
	dpanic("Delete.WithFields")
	d.q.WithFields(filters...)
	return d
}
func (d *Delete) Keys(keys ...nosql.Key) nosql.Delete {
	dpanic("Delete.Keys")
	for _, k := range keys {
		id := toOuchValue("?", k.Value()).(string)
		d.keys = append(d.keys, id)
	}
	return d
}
func (d *Delete) Do(ctx context.Context) error {
	dpanic("Delete.Do")

	if len(d.keys) > 0 {
		d.q.(*Query).ouchQuery["selector"].(map[string]interface{})[idField] = map[string]interface{}{"$in": d.keys}
	}

	// TODO only pull back the _id & _ref fields in the query, or better still used the delete API!

	it := d.q.Iterate().(*Iterator)
	if it.Err() != nil {
		return it.Err()
	}

	deleteSet := make(map[string]string)

	for it.Next(ctx) {
		if it.err != nil {
			return it.err
		}
		id := it.doc[idField].(string)
		rev := it.doc[revField].(string)
		deleteSet[id] = rev
	}
	if err := it.Close(); err != nil {
		return err
	}

	//fmt.Println("DEBUG DELETE set", deleteSet)

	for id, rev := range deleteSet {
		_, err := d.db.db.Delete(ctx, id, rev)
		if err != nil {
			return err
		}
	}

	return nil
}

type Update struct {
	db     *DB
	col    string
	key    nosql.Key
	update nosql.Document
	upsert bool
	inc    map[string]int // increment the named numeric field by the int
}

func (u *Update) Inc(field string, dn int) nosql.Update {
	dpanic("Update.Inc")
	//fmt.Println("DEBUG",field, dn)
	if u.inc == nil {
		u.inc = make(map[string]int)
	}
	u.inc[field] += dn
	return u
}

func (u *Update) Upsert(d nosql.Document) nosql.Update {
	dpanic("Update.Upsert")
	//fmt.Println("DEBUG", d)
	u.upsert = true
	for k, v := range d {
		u.update[k] = v
	}
	return u
}
func (u *Update) Do(ctx context.Context) error {
	dpanic("Update.Do")
	orig, id, rev, err := u.db.findByKey(u.col, u.key)
	if err == nosql.ErrNotFound {
		if !u.upsert {
			return err
		} else {
			var idKey nosql.Key
			orig = u.update
			idKey, rev, err = u.db.insert(u.col, u.key, orig)
			if err != nil {
				return err
			}
			id = toOuchValue(idField, idKey.Value()).(string)
		}
	} else {
		if err != nil {
			return err
		}
		for k, v := range u.update { // alter any changed fields
			orig[k] = v
		}
	}

	for k, v := range u.inc { // increment numerical values
		val, exists := orig[k]
		if exists {
			switch x := val.(type) {
			case nosql.Int:
				val = nosql.Int(int64(x) + int64(v))
			case nosql.Float:
				val = nosql.Float(float64(x) + float64(v))
			default:
				return errors.New("field '" + k + "' is not a number")
			}
		} else {
			val = nosql.Int(v)
		}
		orig[k] = val
	}

	_, err = u.db.db.Put(ctx, compKey(u.col, u.key), toOuchDoc(u.col, id, rev, orig))
	return err
}
