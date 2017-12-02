package ouch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"runtime"
	"strings"

	"github.com/gopherjs/gopherjs/js"
	"github.com/pborman/uuid"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/nosql"

	"github.com/flimzy/kivik"
)

var trace bool

// DEBUG trace rather than panic
func dpanic(s string) {
	if trace {
		//	println(s)
	}
}

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

	driver := defaultDriverName
	// if drivOpt, exists, err := opt.StringKey("driver"); exists {
	// 	driver = drivOpt
	// } else {
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// }

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

	client, err := kivik.New(context.TODO(), driver, dsn)
	if err != nil {
		return nil, err
	}

	if create {
		err := client.CreateDB(context.TODO(), dbName)
		if err != nil {
			return nil, err
		}
	}

	db, err := client.DB(context.TODO(), dbName)
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
	//TODO!
	compPK    bool // compose PK from existing keys; if false, use _id instead of target field
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
	if db.driver == "memory" { // the memory driver does not implement this functionality
		return nil
	}
	// TODO add indexes...
	return nil

}

const idField = "_id"
const revField = "_rev"
const collectionField = "Collection"

func compKeyType(col string, key nosql.Key) nosql.Key {
	return key // ignore col for now
}

func compKey(col string, key nosql.Key) string {
	return toInterfaceValue("?", compKeyType(col, key)).(string)
}

func (db *DB) Insert(col string, key nosql.Key, d nosql.Document) (nosql.Key, error) {
	dpanic("DB.Insert")
	k, _, e := db.insert(col, key, d)
	return k, e
}
func (db *DB) insert(col string, key nosql.Key, d nosql.Document) (nosql.Key, nosql.String, error) {
	dpanic("DB.insert")
	// fmt.Println("DEBUG INSERT", col, key)
	// fmt.Printf("doc: %#v\n", d)

	if d == nil {
		return nil, "", errors.New("no document to insert")
	}
	d[collectionField] = nosql.String(col)

	if key == nil {
		// if col != "log" {
		// 	fmt.Printf("%s doc no key: %#v\n", col, d)
		// }
		key = nosql.Key{uuid.NewUUID().String()}
	}

	cK := compKeyType(col, key)
	d[idField] = cK

	interfaceDoc := toInterfaceDoc(d)

	if trace {
		fmt.Printf("DEBUG insert native %#v\n", interfaceDoc)
	}

	_, rev, err := db.db.CreateDoc(context.TODO(), interfaceDoc)
	if err != nil {
		return nil, "", err
	}

	//fmt.Println("DEBUG returned Insert Key:", key, interfaceDoc)

	return key, nosql.String(rev), nil
}

func (db *DB) FindByKey(col string, key nosql.Key) (nosql.Document, error) {
	dpanic("DB.FindByKey")

	cK := compKey(col, key)

	row, err := db.db.Get(context.TODO(), cK)
	if err != nil {
		if kivik.StatusCode(err) == kivik.StatusNotFound {
			return nil, nosql.ErrNotFound
		}
		return nil, err
	}

	rowDoc := make(map[string]interface{})
	err = row.ScanDoc(&rowDoc)
	if err != nil {
		return nil, err
	}
	decoded := fromInterfaceDoc(rowDoc)

	//fmt.Println("DEBUG", col, key, "FOUND", decoded)

	return decoded, nil
}

func (db *DB) Query(col string) nosql.Query {
	dpanic("DB.Query")
	qry := &Query{db: db, ouchQuery: map[string]interface{}{"selector": map[string]interface{}{}}}
	if col != "" {
		qry.ouchQuery["selector"].(map[string]interface{})[collectionField] = "S" + col
	}
	return qry
}
func (db *DB) Update(col string, key nosql.Key) nosql.Update {
	dpanic("DB.Update")
	//fmt.Println("DEBUG ", col, key)
	return &Update{db: db, col: col, key: key, update: nosql.Document{
		collectionField: nosql.String(col),
	}}
}
func (db *DB) Delete(col string) nosql.Delete {
	dpanic("DB.Delete")

	return &Delete{db: db, col: col, q: db.Query(col)}
}

type ouchQuery map[string]interface{}

type Query struct {
	db *DB
	ouchQuery
}

func (q *Query) WithFields(filters ...nosql.FieldFilter) nosql.Query {
	dpanic("Query.WithFields")

	for _, filter := range filters {
		test := ""
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
			panic("unknown nosqlFilter " + string(rune(filter.Filter)+'0'))
		}
		term := map[string]interface{}{test: toInterfaceValue(".", filter.Value)}
		for w := len(filter.Path) - 1; w >= 0; w-- {
			if w == 0 {
				//term = map[string]interface{}{"$or": []interface{}{term, map[string]interface{}{"val": term}}}
				q.ouchQuery["selector"].(map[string]interface{})[filter.Path[0]] = term
			} else {
				term = map[string]interface{}{filter.Path[w]: term}
			}
		}
	}

	//fmt.Printf("DEBUG query %#v\n", q)
	return q
}

func (q *Query) Limit(n int) nosql.Query {
	dpanic("Query.Limit")
	q.ouchQuery["limit"] = n
	return q
}

func (q *Query) Count(ctx context.Context) (int64, error) {
	dpanic("Query.Count")
	it := q.Iterate()
	defer it.Close()
	var count int64
	for it.Next(ctx) {
		count++
	}
	//fmt.Println("DEBUG count", count)
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
	q.debug()
	rows, err := q.db.db.Find(context.TODO(), q.ouchQuery)
	return &Iterator{rows: rows, err: err}
}

func (q *Query) debug() {
	if trace || false {
		if runtime.GOARCH == "js" {
			query := js.Global.Get("JSON").Call("stringify", q.ouchQuery).String()
			fmt.Println("DEBUG query marshal JS", query)
		} else {
			byts, err := json.Marshal(q.ouchQuery)
			qry := string(byts)
			if strings.Contains(qry, `KK`) {
				fmt.Println("DEBUG query marshal", err, qry)
			}
		}
	}
}

type Iterator struct {
	err  error
	rows *kivik.Rows
}

func (it *Iterator) Next(ctx context.Context) bool {
	dpanic("Iterator.Next")
	// if it.err != nil {
	// 	return false
	// }
	return it.rows.Next()
}
func (it *Iterator) Err() error {
	dpanic("Iterator.Err")
	return it.err
}
func (it *Iterator) Close() error {
	dpanic("Iterator.Close")
	return it.rows.Close()
}
func (it *Iterator) Key() nosql.Key {
	dpanic("Iterator.Key")
	id := it.rows.ID()
	if len(id) == 0 {
		err1 := errors.New("Iterator.Key empty ouch id")
		doc := map[string]interface{}{}
		err2 := it.rows.ScanDoc(&doc)
		fmt.Println("DEBUG Iterator.Key.ScanKey", err1, err2, doc)
		if err2 != nil {
			it.err = err1
			fmt.Println("DEBUG", it.err)
			return nosql.Key{}
		}
		var haveID bool
		id, haveID = doc[idField].(string)
		if !haveID {
			fmt.Printf("DEBUG Iterator.Key doc[idField] %T %v", doc[idField], doc[idField])
			it.err = err1
			return nosql.Key{}
		}
	}
	ret := fromInterfaceValue("?", id).(nosql.Key)
	fmt.Println("DEBUG returned Iterator.Key:", ret)
	return ret
}
func (it *Iterator) Doc() nosql.Document {
	dpanic("Iterator.Doc")
	doc := map[string]interface{}{}
	it.err = it.rows.ScanDoc(&doc)
	if it.err != nil {
		panic(it.err)
	}
	return fromInterfaceDoc(doc)
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
		id := toInterfaceValue("?", k).(string)
		if strings.HasPrefix(id, "KK") {
			id = id[1:] // TODO FIXME!
		}
		d.keys = append(d.keys, id)
	}
	return d
}
func (d *Delete) Do(ctx context.Context) error {
	dpanic("Delete.Do")

	d.q.(*Query).ouchQuery["selector"].(map[string]interface{})[idField] = map[string]interface{}{"$in": d.keys}

	// TODO only pull back the _id & _ref fields in the query

	d.q.(*Query).debug()

	it := d.q.Iterate().(*Iterator)
	if it.Err() != nil {
		return it.Err()
	}
	for it.Next(ctx) {
		var sdoc map[string]interface{}
		err := it.rows.ScanDoc(&sdoc)
		if err != nil {
			return err
		}
		_, err = d.db.db.Delete(ctx, sdoc[idField].(string), sdoc[revField].(string))
		if err != nil {
			return err
		}
	}
	if err := it.Close(); err != nil {
		return err
	}
	return nil
}

type Update struct {
	db     *DB
	col    string
	key    nosql.Key
	update nosql.Document
	upsert bool
	inc    map[string]int         // increment the named numeric field by the int
	push   map[string]nosql.Value // replace the named field with the new value
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

func (u *Update) Push(field string, v nosql.Value) nosql.Update {
	dpanic("Update.Push")
	//fmt.Println("DEBUG", field, v)
	u.push[field] = v
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
	col := ""
	if c, ok := u.update[collectionField]; ok {
		if cs, ok := c.(nosql.String); ok {
			col = string(cs)
		}
	}
	if col == "" {
		return errors.New("no collection name")
	}

	orig, err := u.db.FindByKey(col, u.key)
	if err == nosql.ErrNotFound {
		if !u.upsert {
			return err
		} else {
			orig = u.update
			_, rev, err := u.db.insert(col, u.key, orig)
			if err != nil {
				return err
			}
			orig[revField] = rev
		}
	} else {
		if err != nil {
			return err
		}
		for k, v := range u.update { // alter any changed fields
			orig[k] = v
		}
	}

	for k, v := range u.push { // push new individual values
		orig[k] = v
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

	_, err = u.db.db.Put(ctx, compKey(u.col, u.key), toInterfaceDoc(orig))
	return err
}
