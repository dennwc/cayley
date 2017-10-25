package mongo

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/nosql"
)

const Type = "mongo"

func init() {
	nosql.Register(Type, nosql.Registration{
		NewFunc:      Open,
		InitFunc:     Create,
		IsPersistent: true,
	})
}

func dialMongo(addr string, options graph.Options) (*mgo.Session, error) {
	if connVal, ok := options["session"]; ok {
		if conn, ok := connVal.(*mgo.Session); ok {
			return conn, nil
		}
	}
	if strings.HasPrefix(addr, "mongodb://") || strings.ContainsAny(addr, `@/\`) {
		// full mongodb url
		return mgo.Dial(addr)
	}
	var dialInfo mgo.DialInfo
	dialInfo.Addrs = strings.Split(addr, ",")
	user, ok, err := options.StringKey("username")
	if err != nil {
		return nil, err
	}
	if ok {
		dialInfo.Username = user
		password, ok, err := options.StringKey("password")
		if err != nil {
			return nil, err
		}
		if ok {
			dialInfo.Password = password
		}
	}
	dbName := nosql.DefaultDBName
	val, ok, err := options.StringKey("database_name")
	if err != nil {
		return nil, err
	}
	if ok {
		dbName = val
	}
	dialInfo.Database = dbName
	return mgo.DialWithInfo(&dialInfo)
}

func dialDB(addr string, opt graph.Options) (*DB, error) {
	sess, err := dialMongo(addr, opt)
	if err != nil {
		return nil, err
	}
	return &DB{sess: sess, db: sess.DB("")}, nil
}

func Create(addr string, opt graph.Options) (nosql.Database, error) {
	return dialDB(addr, opt)
}

func Open(addr string, opt graph.Options) (nosql.Database, error) {
	return dialDB(addr, opt)
}

type DB struct {
	sess *mgo.Session
	db   *mgo.Database
}

func (db *DB) Close() error {
	db.sess.Close()
	return nil
}
func (db *DB) EnsureIndex(col string, indexes ...nosql.Index) error {
	c := db.db.C(col)
	for _, ind := range indexes {
		err := c.EnsureIndex(mgo.Index{
			Key:        []string{ind.Field},
			Unique:     false,
			DropDups:   false,
			Background: true,
			Sparse:     true,
		})
		if err != nil {
			return err
		}
	}
	return nil
}
func toBsonValue(v nosql.Value) interface{} {
	switch v := v.(type) {
	case nil:
		return nil
	case nosql.Document:
		return toBsonDoc(v)
	case nosql.Array:
		arr := make([]interface{}, 0, len(v))
		for _, s := range v {
			arr = append(arr, toBsonValue(s))
		}
		return arr
	case nosql.String:
		return string(v)
	case nosql.Int:
		return int64(v)
	case nosql.Float:
		return float64(v)
	case nosql.Bool:
		return bool(v)
	case nosql.Time:
		return time.Time(v)
	case nosql.Bytes:
		return []byte(v)
	default:
		panic(fmt.Errorf("unsupported type: %T", v))
	}
}
func fromBsonValue(v interface{}) nosql.Value {
	switch v := v.(type) {
	case nil:
		return nil
	case bson.M:
		return fromBsonDoc(v)
	case []interface{}:
		arr := make(nosql.Array, 0, len(v))
		for _, s := range v {
			arr = append(arr, fromBsonValue(s))
		}
		return arr
	case bson.ObjectId:
		return nosql.String(objidString(v))
	case string:
		return nosql.String(v)
	case int:
		return nosql.Int(v)
	case int64:
		return nosql.Int(v)
	case float64:
		return nosql.Float(v)
	case bool:
		return nosql.Bool(v)
	case time.Time:
		return nosql.Time(v)
	case []byte:
		return nosql.Bytes(v)
	default:
		panic(fmt.Errorf("unsupported type: %T", v))
	}
}
func toBsonDoc(d nosql.Document) bson.M {
	if d == nil {
		return nil
	}
	m := make(bson.M, len(d))
	for k, v := range d {
		m[k] = toBsonValue(v)
	}
	return m
}
func fromBsonDoc(d bson.M) nosql.Document {
	if d == nil {
		return nil
	}
	m := make(nosql.Document, len(d))
	for k, v := range d {
		m[k] = fromBsonValue(v)
	}
	return m
}

const idField = "_id"

func convRootDoc(m bson.M) nosql.Document {
	d := fromBsonDoc(m)
	if id, ok := d[idField]; ok {
		delete(d, idField)
		d[nosql.IDField] = id
	}
	return d
}

func objidString(id bson.ObjectId) string {
	return hex.EncodeToString([]byte(id))
}

func (db *DB) Insert(col string, id string, d nosql.Document) (string, error) {
	m := toBsonDoc(d)
	var mid interface{}
	if id == "" {
		oid := bson.NewObjectId()
		mid = oid
		id = objidString(oid)
	} else {
		mid = id
	}
	m[idField] = mid
	delete(m, nosql.IDField)
	if err := db.db.C(col).Insert(m); err != nil {
		return "", err
	}
	return id, nil
}
func (db *DB) FindByID(col string, id string) (nosql.Document, error) {
	var m bson.M
	err := db.db.C(col).FindId(id).One(&m)
	if err == mgo.ErrNotFound {
		return nil, nosql.ErrNotFound
	} else if err != nil {
		return nil, err
	}
	return fromBsonDoc(m), nil
}
func (db *DB) Query(col string) nosql.Query {
	return &Query{col: db.db.C(col)}
}
func (db *DB) Update(col string, id string) nosql.Update {
	return &Update{col: db.db.C(col), id: id, update: make(bson.M)}
}
func (db *DB) Delete(col string) nosql.Delete {
	return &Delete{col: db.db.C(col)}
}

func buildFilters(filters []nosql.FieldFilter) bson.M {
	m := make(bson.M, len(filters))
	for _, f := range filters {
		v := toBsonValue(f.Value)
		var mf interface{}
		switch f.Filter {
		case nosql.Equal:
			mf = v
		case nosql.NotEqual:
			mf = bson.M{"$ne": v}
		case nosql.GT:
			mf = bson.M{"$gt": v}
		case nosql.GTE:
			mf = bson.M{"$gte": v}
		case nosql.LT:
			mf = bson.M{"$lt": v}
		case nosql.LTE:
			mf = bson.M{"$lte": v}
		default:
			panic(fmt.Errorf("unsupported filter: %v", f.Filter))
		}
		m[strings.Join(f.Path, ".")] = mf
	}
	return m
}

func mergeFilters(dst, src bson.M) {
	for k, v := range src {
		dst[k] = v
	}
}

type Query struct {
	col   *mgo.Collection
	limit int
	query bson.M
}

func (q *Query) WithFields(filters ...nosql.FieldFilter) nosql.Query {
	m := buildFilters(filters)
	if q.query == nil {
		q.query = m
	} else {
		mergeFilters(q.query, m)
	}
	return q
}
func (q *Query) Limit(n int) nosql.Query {
	q.limit = n
	return q
}
func (q *Query) build() *mgo.Query {
	var m interface{}
	if q.query != nil {
		m = q.query
	}
	qu := q.col.Find(m)
	if q.limit > 0 {
		qu = qu.Limit(q.limit)
	}
	return qu
}
func (q *Query) Count(ctx context.Context) (int64, error) {
	n, err := q.build().Count()
	return int64(n), err
}
func (q *Query) One(ctx context.Context) (nosql.Document, error) {
	var m bson.M
	err := q.build().One(&m)
	if err == mgo.ErrNotFound {
		return nil, nosql.ErrNotFound
	} else if err != nil {
		return nil, err
	}
	return convRootDoc(m), nil
}
func (q *Query) Iterate() nosql.DocIterator {
	it := q.build().Iter()
	return &Iterator{it: it}
}

type Iterator struct {
	it  *mgo.Iter
	res bson.M
}

func (it *Iterator) Next(ctx context.Context) bool {
	it.res = make(bson.M)
	return it.it.Next(&it.res)
}
func (it *Iterator) Err() error {
	return it.it.Err()
}
func (it *Iterator) Close() error {
	return it.it.Close()
}
func (it *Iterator) ID() string {
	s, _ := fromBsonValue(it.res[idField]).(nosql.String)
	return string(s)
}
func (it *Iterator) Doc() nosql.Document {
	return convRootDoc(it.res)
}

type Delete struct {
	col   *mgo.Collection
	query bson.M
}

func (d *Delete) WithFields(filters ...nosql.FieldFilter) nosql.Delete {
	m := buildFilters(filters)
	if d.query == nil {
		d.query = m
	} else {
		mergeFilters(d.query, m)
	}
	return d
}
func (d *Delete) IDs(ids ...string) nosql.Delete {
	if len(ids) == 0 {
		return d
	}
	m := make(bson.M, 1)
	if len(ids) == 1 {
		m[idField] = ids[0]
	} else {
		m[idField] = bson.M{"$in": ids}
	}
	if d.query == nil {
		d.query = m
	} else {
		mergeFilters(d.query, m)
	}
	return d
}
func (d *Delete) Do(ctx context.Context) error {
	var qu interface{}
	if d.query != nil {
		qu = d.query
	}
	_, err := d.col.RemoveAll(qu)
	return err
}

type Update struct {
	col    *mgo.Collection
	id     string
	upsert bson.M
	update bson.M
}

func (u *Update) Inc(field string, dn int) nosql.Update {
	inc, _ := u.update["$inc"].(bson.M)
	if inc == nil {
		inc = make(bson.M)
	}
	inc[field] = dn
	u.update["$inc"] = inc
	return u
}
func (u *Update) Push(field string, v nosql.Value) nosql.Update {
	push, _ := u.update["$push"].(bson.M)
	if push == nil {
		push = make(bson.M)
	}
	push[field] = toBsonValue(v)
	u.update["$push"] = push
	return u
}
func (u *Update) Upsert(d nosql.Document) nosql.Update {
	u.upsert = toBsonDoc(d)
	if u.upsert == nil {
		u.upsert = make(bson.M)
	}
	return u
}
func (u *Update) Do(ctx context.Context) error {
	var err error
	if u.upsert != nil {
		if len(u.upsert) != 0 {
			u.update["$setOnInsert"] = u.upsert
		}
		_, err = u.col.UpsertId(u.id, u.update)
	} else {
		err = u.col.UpdateId(u.id, u.update)
	}
	return err
}
