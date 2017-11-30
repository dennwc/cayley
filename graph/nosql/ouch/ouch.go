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

// DEBUG trace rather than panic
func dpanic(s string) {
	println(s)
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

	driver := defaultDriverName
	// if drivOpt, exists, err := opt.StringKey("driver"); exists {
	// 	driver = drivOpt
	// } else {
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// }
	fmt.Println("DEBUG dialDB", driver, create, addr, opt)

	addrParsed, err := url.Parse(addr)
	if err != nil {
		return nil, err
	}

	pathParts := strings.Split(addrParsed.Path, "/")
	dbName := "cayleygraph"
	if len(pathParts) > 0 && addr != "" {
		dbName = pathParts[len(pathParts)-1]
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
	create := false
	// if opt["driver"] == "memory" { // a blank database, intended for testing
	// 	create = true
	// }
	return dialDB(create, addr, opt)
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
	// seems no way to close a kivik session, so just let the go garbage collection do its stuff...
	db.db = nil
	db.colls = nil
	return nil
}

func (db *DB) EnsureIndex(col string, primary nosql.Index, secondary []nosql.Index) error {
	if primary.Type != nosql.StringExact {
		return fmt.Errorf("unsupported type of primary index: %v", primary.Type)
	}
	fmt.Println("DEBUG DB.EnsureIndex", col, primary, secondary)
	if db.driver != "memory" { // the memory driver does not implement this functionality
		panic("DB.EnsureIndex")
	}
	return nil

}

const idField = "_id"
const revField = "_rev"
const collectionField = "Collection"

func compKey(key []string) string {
	return strings.Join(key, "")
}

func (db *DB) Insert(col string, key nosql.Key, d nosql.Document) (nosql.Key, error) {
	dpanic("DB.Insert")
	k, _, e := db.insert(col, key, d)
	return k, e
}
func (db *DB) insert(col string, key nosql.Key, d nosql.Document) (nosql.Key, nosql.String, error) {
	dpanic("DB.insert")
	fmt.Println("DEBUG INSERT", col, key)
	fmt.Printf("doc: %#v\n", d)

	if d == nil {
		return nil, "", errors.New("no document to insert")
	}
	d[collectionField] = nosql.String(col)

	cK := compKey(key)
	if cK != "" {
		d[idField] = nosql.String(cK)
	}

	interfaceDoc := toInterfaceDoc(d)

	fmt.Println("DEBUG", interfaceDoc)

	ouchID, rev, err := db.db.CreateDoc(context.TODO(), interfaceDoc)
	if err != nil {
		return nil, "", err
	}

	if cK == "" {
		key = nosql.Key([]string{ouchID}) // key auto-created
		fmt.Println("DEBUG Created:", key)
	}

	// if err := db.dcheck(key, d); err != nil {
	// 	return nil, "", err
	// }

	return key, nosql.String(rev), nil
}

func (db *DB) FindByKey(col string, key nosql.Key) (nosql.Document, error) {
	dpanic("DB.FindByKey")

	cK := compKey(key)

	row, err := db.db.Get(context.TODO(), cK)
	if err != nil {
		fmt.Println("DEBUG", col, key, "NOT FOUND")
		return nil, nosql.ErrNotFound
		//return nil, err
	}

	rowDoc := make(map[string]interface{})
	err = row.ScanDoc(&rowDoc)
	if err != nil {
		return nil, err
	}
	decoded := fromInterfaceDoc(rowDoc)

	fmt.Println("DEBUG", col, key, "FOUND", decoded)

	return decoded, nil
}

func (db *DB) Query(col string) nosql.Query {
	dpanic("DB.Query")
	return &Query{col: col}
}
func (db *DB) Update(col string, key nosql.Key) nosql.Update {
	dpanic("DB.Update")
	fmt.Println("DEBUG ", col, key)
	return &Update{db: db, col: col, key: key, update: nosql.Document{
		collectionField: nosql.String(col),
	}}
}
func (db *DB) Delete(col string) nosql.Delete {
	dpanic("DB.Delete")

	return &Delete{col: col}
}

type Query struct {
	col   string
	limit int
	//query TODO
	filters []nosql.FieldFilter
}

func (q *Query) WithFields(filters ...nosql.FieldFilter) nosql.Query {
	dpanic("Query.WithFields")
	q.filters = append(q.filters, filters...)
	return q
}

func (q *Query) Limit(n int) nosql.Query {
	dpanic("Query.Limit")
	q.limit = n
	return q
}

func (q *Query) Count(ctx context.Context) (int64, error) {
	panic("Query.Count")

	return 0, nil
}
func (q *Query) One(ctx context.Context) (nosql.Document, error) {
	panic("Query.One")

	return nil, nil
}
func (q *Query) Iterate() nosql.DocIterator {
	panic("Query.Iterate")

	return &Iterator{}
}

type Iterator struct {
	c *collection
}

func (it *Iterator) Next(ctx context.Context) bool {
	panic("Iterator.Next")
	return false
}
func (it *Iterator) Err() error {
	panic("Iterator.Err")
	return nil
}
func (it *Iterator) Close() error {
	panic("Iterator.Close")
	return nil
}
func (it *Iterator) Key() nosql.Key {
	panic("Iterator.Key")
	return nil
}
func (it *Iterator) Doc() nosql.Document {
	panic("Iterator.Doc")
	return nil
}

type Delete struct {
	col string
	// query TODO
}

func (d *Delete) WithFields(filters ...nosql.FieldFilter) nosql.Delete {
	panic("Delete.WithFields")
	return nil
}
func (d *Delete) Keys(keys ...nosql.Key) nosql.Delete {
	panic("Delete.Keys")
	return nil
}
func (d *Delete) Do(ctx context.Context) error {
	panic("Delete.Do")
	return nil
}

type Update struct {
	db     *DB
	col    string
	key    nosql.Key
	update nosql.Document
	upsert bool
	inc    map[string]int // is this the required logic???
	push   map[string]nosql.Value
}

func (u *Update) Inc(field string, dn int) nosql.Update {
	dpanic("Update.Inc")
	fmt.Println(field, dn)
	if u.inc == nil {
		u.inc = make(map[string]int)
	}
	u.inc[field] += dn
	return u
}

func (u *Update) Push(field string, v nosql.Value) nosql.Update {
	dpanic("Update.Push")
	fmt.Println("DEBUG", field, v)
	u.push[field] = v
	return u
}
func (u *Update) Upsert(d nosql.Document) nosql.Update {
	dpanic("Update.Upsert")
	fmt.Println("DEBUG", d)
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
		return err
	}

	for k, v := range u.update {
		orig[k] = v
	}

	for k, v := range u.push { // TODO is this right?
		orig[k] = v
	}

	for k, v := range u.inc { // TODO is this right?
		val, exists := orig[k]
		if exists {
			switch x := val.(type) {
			case nosql.Int:
				val = nosql.Int(int64(x) + int64(v))
			default:
				return errors.New("field '" + k + "' is not an integer")
			}
		} else {
			val = nosql.Int(v)
		}
		orig[k] = val
	}

	_, err = u.db.db.Put(context.TODO(), compKey(u.key), toInterfaceDoc(orig))
	if err != nil {
		return err
	}

	// if err := u.db.dcheck(u.key, orig); err != nil {
	// 	return err
	// }

	return err
}
