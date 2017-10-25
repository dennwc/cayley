package dynamo

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/nosql"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

const Type = "dynamo"

var (
	_ nosql.BatchInserter = (*DB)(nil)
)

func init() {
	nosql.Register(Type, nosql.Registration{
		NewFunc:      Open,
		InitFunc:     Create,
		IsPersistent: true,
	})
}

func newSession(access, secret, region string) (*session.Session, error) {
	cred := credentials.NewStaticCredentials(access, secret, "")
	return session.NewSession(&aws.Config{
		Region:      aws.String(region),
		Credentials: cred,
	})
}

func newDB(addr string, opt graph.Options) (*DB, error) {
	acc, _, _ := opt.StringKey("accessKey")
	sec, _, _ := opt.StringKey("secretKey")
	reg, _, _ := opt.StringKey("region")
	sess, err := newSession(acc, sec, reg)
	if err != nil {
		return nil, err
	}
	if addr == "" {
		addr = nosql.DefaultDBName
	}
	return &DB{
		pref: addr + "_", db: dynamodb.New(sess),
		tables: make(map[string]table),
	}, nil
}

func Create(addr string, opt graph.Options) (nosql.Database, error) {
	return newDB(addr, opt)
}

func Open(addr string, opt graph.Options) (nosql.Database, error) {
	return newDB(addr, opt)
}

func convError(err error) error {
	if e, ok := err.(awserr.Error); ok {
		switch e.Code() {
		case "ResourceNotFoundException":
			return nosql.ErrNotFound
			//case "ConditionalCheckFailedException":
		}
	}
	return err
}

type table struct {
	name      *string
	compRange bool // range key composed from multiple fields
	primary   nosql.Index
	secondary []nosql.Index
}

func (t *table) rangeKey(key nosql.Key) *dynamodb.AttributeValue {
	return str(strings.Join([]string(key[1:]), ""))
}

func (t *table) getKey(key nosql.Key) map[string]*dynamodb.AttributeValue {
	keys := make(map[string]*dynamodb.AttributeValue, 2)
	fields := t.primary.Fields
	keys[fields[0]] = str(key[0])
	if len(fields) == 2 {
		keys[fields[1]] = str(key[1])
	} else if len(fields) > 1 {
		keys[fldRange] = t.rangeKey(key)
	}
	return keys
}

func (t *table) convDoc(m map[string]*dynamodb.AttributeValue) nosql.Document {
	if len(t.primary.Fields) > 2 {
		delete(m, fldRange)
	}
	return fromAwsDoc(m)
}

func (t *table) insert(key nosql.Key, d nosql.Document) (string, map[string]*string, map[string]*dynamodb.AttributeValue) {
	item := toAwsDoc(d)
	for i, f := range t.primary.Fields {
		item[f] = str(key[i])
	}
	if t.compRange {
		item[fldRange] = t.rangeKey(key)
	}
	cond := make([]string, 0, len(t.primary.Fields))
	names := make(map[string]*string, cap(cond))
	for i, f := range t.primary.Fields {
		name := fmt.Sprintf("k%d", i+1)
		cond = append(cond, "attribute_not_exists(#"+name+")")
		names["#"+name] = aws.String(f)
	}
	return strings.Join(cond, " AND "), names, item
}

type DB struct {
	pref   string
	db     *dynamodb.DynamoDB
	tables map[string]table
}

func (db *DB) col(name string) *string {
	return aws.String(db.pref + name)
}
func (db *DB) Close() error {
	return nil
}

func (db *DB) waitTable(ctx context.Context, name *string) error {
	for {
		resp, err := db.db.DescribeTable(&dynamodb.DescribeTableInput{
			TableName: name,
		})
		if err != nil {
			return err
		}
		switch *resp.Table.TableStatus {
		case "ACTIVE":
			return nil
		case "DELETING":
			return errors.New("table is being deleted")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

const fldRange = "range"

func (db *DB) EnsureIndex(ctx context.Context, col string, primary nosql.Index, secondary []nosql.Index) error {
	if primary.Type != nosql.StringExact {
		return fmt.Errorf("unsupported type of primary index: %v", primary.Type)
	}
	tbl := db.col(col)

	var (
		attrs []*dynamodb.AttributeDefinition
		keys  []*dynamodb.KeySchemaElement
	)
	for i, f := range primary.Fields {
		if i > 1 {
			// dynamo supports only 2 fields in pk
			// we will compute the second one by concatenating other fields
			attrs[1].AttributeName = aws.String(fldRange)
			keys[1].AttributeName = aws.String(fldRange)
			break
		}
		kt := "RANGE"
		if i == 0 {
			kt = "HASH"
		}
		attrs = append(attrs, &dynamodb.AttributeDefinition{
			AttributeName: aws.String(f),
			AttributeType: aws.String("S"),
		})
		keys = append(keys, &dynamodb.KeySchemaElement{
			AttributeName: aws.String(f),
			KeyType:       aws.String(kt),
		})
	}

	_, err := db.db.CreateTableWithContext(ctx, &dynamodb.CreateTableInput{
		TableName:            tbl,
		AttributeDefinitions: attrs,
		KeySchema:            keys,
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		},
	})
	if e, ok := err.(awserr.Error); ok && e.Code() == "ResourceInUseException" {
		err = nil // already exists
	}
	if err != nil {
		return err
	}
	if err = db.waitTable(ctx, tbl); err != nil {
		return err
	}
	db.tables[col] = table{
		name:      tbl,
		compRange: len(primary.Fields) > 2,
		primary:   primary,
		secondary: secondary,
	}
	return nil
}

const timeFormat = time.RFC3339Nano

func str(s string) *dynamodb.AttributeValue {
	return &dynamodb.AttributeValue{S: aws.String(s)}
}

func int64Attr(v int64) *dynamodb.AttributeValue {
	return &dynamodb.AttributeValue{N: aws.String(strconv.FormatInt(int64(v), 10))}
}

const (
	fldType = "_type"
	fldVal  = "_val"

	typeTime = "time"
)

func toAwsValue(v nosql.Value) *dynamodb.AttributeValue {
	switch v := v.(type) {
	case nil:
		return &dynamodb.AttributeValue{NULL: aws.Bool(true)}
	case nosql.Strings:
		ss := make([]*string, 0, len(v))
		for _, k := range v {
			ss = append(ss, aws.String(k))
		}
		return &dynamodb.AttributeValue{SS: ss}
	case nosql.Document:
		_, ok1 := v[fldType]
		_, ok2 := v[fldVal]
		if ok1 || ok2 {
			panic(fmt.Errorf("document collides with internal types: %v", v))
		}
		return &dynamodb.AttributeValue{M: toAwsDoc(v)}
	case nosql.String:
		return str(string(v))
	case nosql.Int:
		return int64Attr(int64(v))
	case nosql.Float:
		// save float value in exp notation so they never be mistaken with ints
		return &dynamodb.AttributeValue{N: aws.String(strconv.FormatFloat(float64(v), 'e', -1, 64))}
	case nosql.Bool:
		return &dynamodb.AttributeValue{BOOL: aws.Bool(bool(v))}
	case nosql.Time:
		return &dynamodb.AttributeValue{M: map[string]*dynamodb.AttributeValue{
			fldType: str(typeTime),
			fldVal:  str(time.Time(v).Format(timeFormat)),
		}}
	case nosql.Bytes:
		return &dynamodb.AttributeValue{B: []byte(v)}
	default:
		panic(fmt.Errorf("unsupported type: %T", v))
	}
}
func fromAwsValue(v *dynamodb.AttributeValue) nosql.Value {
	if v == nil {
		return nil
	}
	switch {
	case v.NULL != nil && *v.NULL:
		return nil
	case v.SS != nil:
		key := make(nosql.Strings, 0, len(v.SS))
		for _, s := range v.SS {
			key = append(key, *s)
		}
		return key
	case v.M != nil:
		if typ := v.M[fldType]; typ != nil && typ.S != nil {
			val := v.M[fldVal]
			var v string
			if val != nil && val.S != nil {
				v = *val.S
			}
			switch *typ.S {
			case typeTime:
				t, _ := time.Parse(timeFormat, v)
				return nosql.Time(t)
			default:
				panic(fmt.Errorf("unsupported data type: %q", *typ.S))
			}
		}
		return fromAwsDoc(v.M)
	case v.S != nil:
		return nosql.String(*v.S)
	case v.N != nil:
		if iv, err := strconv.ParseInt(*v.N, 10, 64); err == nil {
			return nosql.Int(iv)
		}
		fv, _ := strconv.ParseFloat(*v.N, 64)
		return nosql.Float(fv)
	case v.BOOL != nil:
		return nosql.Bool(*v.BOOL)
	case v.B != nil:
		return nosql.Bytes(v.B)
	default:
		panic(fmt.Errorf("unsupported type: %+v", v))
	}
}
func toAwsDoc(d nosql.Document) map[string]*dynamodb.AttributeValue {
	if d == nil {
		return nil
	}
	m := make(map[string]*dynamodb.AttributeValue, len(d))
	for k, v := range d {
		m[k] = toAwsValue(v)
	}
	return m
}
func fromAwsDoc(d map[string]*dynamodb.AttributeValue) nosql.Document {
	if d == nil {
		return nil
	}
	m := make(nosql.Document, len(d))
	for k, v := range d {
		m[k] = fromAwsValue(v)
	}
	return m
}

func (db *DB) Insert(ctx context.Context, col string, key nosql.Key, d nosql.Document) (nosql.Key, error) {
	if key == nil {
		key = nosql.GenKey()
	}
	tbl := db.tables[col]
	if len(key) != len(tbl.primary.Fields) {
		return nil, fmt.Errorf("unexpected key length: got %d, exp: %d", len(key), len(tbl.primary.Fields))
	}
	cond, names, item := tbl.insert(key, d)
	_, err := db.db.PutItemWithContext(ctx, &dynamodb.PutItemInput{
		TableName:                tbl.name,
		ConditionExpression:      aws.String(cond),
		ExpressionAttributeNames: names,
		Item: item,
	})
	if err != nil {
		return nil, err
	}
	return key, nil
}
func (db *DB) FindByKey(ctx context.Context, col string, key nosql.Key) (nosql.Document, error) {
	tbl := db.tables[col]
	resp, err := db.db.GetItemWithContext(ctx, &dynamodb.GetItemInput{
		TableName: tbl.name,
		Key:       tbl.getKey(key),
	})
	if err != nil {
		return nil, convError(err)
	} else if len(resp.Item) == 0 {
		return nil, nosql.ErrNotFound
	}
	return tbl.convDoc(resp.Item), nil
}
func (db *DB) Query(col string) nosql.Query {
	tbl := db.tables[col]
	return &Query{db: db.db, tbl: &tbl, f: newFilter()}
}
func (db *DB) Update(col string, key nosql.Key) nosql.Update {
	tbl := db.tables[col]
	return &Update{
		db: db.db, tbl: &tbl, key: key,
		names: make(map[string]*string),
		vals:  make(map[string]*dynamodb.AttributeValue),
	}
}
func (db *DB) Delete(col string) nosql.Delete {
	tbl := db.tables[col]
	return &Delete{db: db.db, tbl: &tbl, f: newFilter()}
}

func newFilter() Filter {
	return Filter{
		names: make(map[string]*string),
		vals:  make(map[string]*dynamodb.AttributeValue),
	}
}

type Filter struct {
	cond  []string
	names map[string]*string
	vals  map[string]*dynamodb.AttributeValue
	last  int
}

func (fl *Filter) nextName() string {
	fl.last++
	n := fl.last
	return fmt.Sprintf("p%d", n)
}
func (fl *Filter) Expr() *string {
	if len(fl.cond) == 0 {
		return nil
	}
	return aws.String(strings.Join(fl.cond, " AND "))
}
func (fl *Filter) Names() map[string]*string {
	if len(fl.names) == 0 {
		return nil
	}
	return fl.names
}
func (fl *Filter) Values() map[string]*dynamodb.AttributeValue {
	if len(fl.vals) == 0 {
		return nil
	}
	return fl.vals
}

func cloneStrings(arr []string) []string {
	out := make([]string, len(arr))
	copy(out, arr)
	return out
}

func (fl *Filter) Append(filters []nosql.FieldFilter) {
	for _, f := range filters {
		if t, ok := f.Value.(nosql.Time); ok {
			// time is different - we need to check the type and change field name
			fl.Append([]nosql.FieldFilter{
				{
					Path:   append(cloneStrings(f.Path), fldType),
					Filter: nosql.Equal,
					Value:  nosql.String(typeTime),
				},
				{
					Path:   append(cloneStrings(f.Path), fldVal),
					Filter: f.Filter,
					Value:  nosql.String(time.Time(t).Format(timeFormat)),
				},
			})
			continue
		}
		var (
			op string
		)
		switch f.Filter {
		case nosql.Equal:
			op = "="
		case nosql.NotEqual:
			op = "<>"
		case nosql.GT:
			op = ">"
		case nosql.GTE:
			op = ">="
		case nosql.LT:
			op = "<"
		case nosql.LTE:
			op = "<="
		default:
			panic(fmt.Errorf("unsupported filter: %v", f.Filter))
		}
		// escape field names to avoid reserved words
		names := make([]string, 0, len(f.Path))
		for _, name := range f.Path {
			pname := fl.nextName()
			fl.names["#"+pname] = aws.String(name)
			names = append(names, "#"+pname)
		}
		fname := strings.Join(names, ".")

		// add a placeholder for value
		vname := fl.nextName()
		val := toAwsValue(f.Value)
		fl.vals[":"+vname] = val

		// and finally add filter
		cond := strings.Join([]string{"(", fname, op, ":" + vname, ")"}, " ")
		fl.cond = append(fl.cond, cond)
	}
}

type Query struct {
	db    *dynamodb.DynamoDB
	tbl   *table
	limit int

	f Filter
}

func (q *Query) WithFields(filters ...nosql.FieldFilter) nosql.Query {
	q.f.Append(filters)
	return q
}
func (q *Query) Limit(n int) nosql.Query {
	q.limit = n
	return q
}
func (q *Query) build() *dynamodb.ScanInput {
	// TODO: split id into hash and range keys and run Query instead of Scan
	qu := &dynamodb.ScanInput{
		TableName:                 q.tbl.name,
		FilterExpression:          q.f.Expr(),
		ExpressionAttributeNames:  q.f.Names(),
		ExpressionAttributeValues: q.f.Values(),
	}
	if q.limit > 0 {
		qu.Limit = aws.Int64(int64(q.limit))
	}
	return qu
}
func (q *Query) Count(ctx context.Context) (int64, error) {
	qu := q.build()
	qu.Select = aws.String(dynamodb.SelectCount)
	resp, err := q.db.Scan(qu)
	if err != nil {
		return 0, err
	}
	return *resp.Count, nil
}
func (q *Query) One(ctx context.Context) (nosql.Document, error) {
	qu := q.build()
	qu.Limit = aws.Int64(1)
	resp, err := q.db.Scan(qu)
	if err != nil {
		return nil, err
	} else if len(resp.Items) == 0 {
		return nil, nosql.ErrNotFound
	}
	return q.tbl.convDoc(resp.Items[0]), nil
}
func (q *Query) Iterate() nosql.DocIterator {
	qu := q.build()
	return &Iterator{db: q.db, q: qu, tbl: q.tbl}
}

type Iterator struct {
	db  *dynamodb.DynamoDB
	tbl *table
	q   *dynamodb.ScanInput

	buf []map[string]*dynamodb.AttributeValue
	i   int
	err error
}

func (it *Iterator) Next(ctx context.Context) bool {
	if it.i+1 < len(it.buf) {
		it.i++
		return true
	} else if it.q == nil {
		return false
	}
	for {
		resp, err := it.db.ScanWithContext(ctx, it.q)
		if err != nil {
			it.err = err
			return false
		}
		it.i = 0
		it.buf = resp.Items
		if resp.LastEvaluatedKey == nil {
			it.q = nil
		} else {
			it.q.ExclusiveStartKey = resp.LastEvaluatedKey
		}
		if len(it.buf) > 0 {
			return true
		} else if it.q == nil {
			return false
		}
	}
}
func (it *Iterator) Err() error {
	return it.err
}
func (it *Iterator) Close() error {
	it.buf = nil
	return nil
}
func (it *Iterator) item() map[string]*dynamodb.AttributeValue {
	if it.i < len(it.buf) {
		return it.buf[it.i]
	}
	return nil
}
func (it *Iterator) Key() nosql.Key {
	m := it.item()
	if m == nil {
		return nil
	}
	key := make(nosql.Key, 0, len(it.tbl.primary.Fields))
	for _, f := range it.tbl.primary.Fields {
		v := ""
		if av := m[f]; av != nil && av.S != nil {
			v = *av.S
		}
		key = append(key, v)
	}
	return key
}
func (it *Iterator) Doc() nosql.Document {
	return it.tbl.convDoc(it.item())
}

type Delete struct {
	db   *dynamodb.DynamoDB
	tbl  *table
	keys []nosql.Key
	f    Filter
}

func (d *Delete) WithFields(filters ...nosql.FieldFilter) nosql.Delete {
	d.f.Append(filters)
	return d
}
func (d *Delete) Keys(keys ...nosql.Key) nosql.Delete {
	d.keys = append(d.keys, keys...)
	return d
}
func (d *Delete) Do(ctx context.Context) error {
	req := dynamodb.DeleteItemInput{
		TableName:                 d.tbl.name,
		ConditionExpression:       d.f.Expr(),
		ExpressionAttributeNames:  d.f.Names(),
		ExpressionAttributeValues: d.f.Values(),
	}

	delOne := func(key nosql.Key) error {
		r := req
		r.Key = d.tbl.getKey(key)
		// TODO: batch deletes
		_, err := d.db.DeleteItemWithContext(ctx, &r)
		if e, ok := err.(awserr.Error); ok {
			switch e.Code() {
			case "ConditionalCheckFailedException":
				err = nil
			}
		}
		return err
	}

	if len(d.keys) == 0 {
		qu := &Query{db: d.db, tbl: d.tbl, f: d.f}
		it := qu.Iterate()
		for it.Next(ctx) {
			if err := delOne(it.Key()); err != nil {
				return err
			}
		}
		return it.Err()
	}
	for _, key := range d.keys {
		if err := delOne(key); err != nil {
			return err
		}
	}
	return nil
}

type Update struct {
	db  *dynamodb.DynamoDB
	tbl *table
	key nosql.Key

	add    []string
	set    []string
	names  map[string]*string
	vals   map[string]*dynamodb.AttributeValue
	upsert bool
}

func (u *Update) Inc(field string, dn int) nosql.Update {
	fl := strings.ToLower(field)
	u.names["#"+fl] = aws.String(field)
	u.vals[":"+fl] = int64Attr(int64(dn))
	u.add = append(u.add, "#"+fl+" :"+fl)
	return u
}
func (u *Update) Push(field string, v nosql.Value) nosql.Update {
	fl := strings.ToLower(field)
	u.names["#"+fl] = aws.String(field)
	var av *dynamodb.AttributeValue
	switch v := v.(type) {
	case nosql.Int:
		av = &dynamodb.AttributeValue{NS: []*string{int64Attr(int64(v)).N}}
	case nosql.String:
		av = &dynamodb.AttributeValue{SS: []*string{aws.String(string(v))}}
	default:
		// TODO: this most probably will lead to error, but we don't use this code path
		av = &dynamodb.AttributeValue{L: []*dynamodb.AttributeValue{
			toAwsValue(v),
		}}
	}
	u.vals[":"+fl] = av
	u.add = append(u.add, "#"+fl+" :"+fl)
	return u
}
func (u *Update) Upsert(d nosql.Document) nosql.Update {
	u.upsert = true
	upsert := toAwsDoc(d)
	if u.tbl.compRange {
		delete(upsert, fldRange)
	}
	for i, f := range u.tbl.primary.Fields {
		if u.tbl.compRange && i != 0 {
			// we should not remove pk fields for composite range key case
			break
		}
		delete(upsert, f)
	}
	for k, v := range upsert {
		kl := strings.ToLower(k)
		if _, ok := u.names["#"+kl]; !ok {
			u.names["#"+kl] = aws.String(k)
			u.vals[":"+kl] = v
			u.set = append(u.set, "#"+kl+" = :"+kl)
		}
	}
	return u
}
func (u *Update) Do(ctx context.Context) error {
	req := &dynamodb.UpdateItemInput{
		TableName: u.tbl.name,
		Key:       u.tbl.getKey(u.key),
	}
	if len(u.names) != 0 {
		req.ExpressionAttributeNames = u.names
		req.ExpressionAttributeValues = u.vals
	}
	if !u.upsert {
		cond := make([]string, 0, 2)

		for i, f := range u.tbl.primary.Fields {
			name := fmt.Sprintf("pk%d", i)
			cond = append(cond, "attribute_exists(#"+name+")")
			if i == 1 && u.tbl.compRange {
				req.ExpressionAttributeNames["#"+name] = aws.String(fldRange)
				break
			}
			req.ExpressionAttributeNames["#"+name] = aws.String(f)
		}
		req.ConditionExpression = aws.String(strings.Join(cond, " AND "))
	}
	if u.tbl.compRange {
		for i, f := range u.tbl.primary.Fields[1:] {
			kl := fmt.Sprintf("rk%d", i)
			u.names["#"+kl] = aws.String(f)
			u.vals[":"+kl] = str(u.key[i+1])
			u.set = append(u.set, "#"+kl+" = :"+kl)
		}
	}
	upd := ""
	if len(u.add) != 0 {
		upd += "ADD " + strings.Join(u.add, ", ") + " "
	}
	if len(u.set) != 0 {
		upd += "SET " + strings.Join(u.set, ", ") + " "
	}
	req.UpdateExpression = aws.String(upd)
	_, err := u.db.UpdateItem(req)
	return err
}

func (db *DB) BatchInsert(col string) nosql.DocWriter {
	tbl := db.tables[col]
	return &inserter{db: db.db, tbl: &tbl}
}

const batchSize = 25

type inserter struct {
	db  *dynamodb.DynamoDB
	tbl *table

	buf   []*dynamodb.WriteRequest
	ikeys []nosql.Key
	keys  []nosql.Key
	err   error
}

func (w *inserter) WriteDoc(ctx context.Context, key nosql.Key, d nosql.Document) error {
	if len(w.buf) >= batchSize {
		if err := w.Flush(ctx); err != nil {
			return err
		}
	}
	if key == nil {
		key = nosql.GenKey()
		_, _, item := w.tbl.insert(key, d)
		// we ignore conditional expression here, since we generated our own key
		w.buf = append(w.buf, &dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{Item: item},
		})
		w.ikeys = append(w.ikeys, key)
		return nil
	}
	// force flush to guarantee correct keys order in case we buffered some writes
	if err := w.Flush(ctx); err != nil {
		return err
	}
	cond, names, item := w.tbl.insert(key, d)
	// dynamo can't check unique key constraint on batch insert
	// we will use it only for ids that we generated
	_, err := w.db.PutItemWithContext(ctx, &dynamodb.PutItemInput{
		TableName:                w.tbl.name,
		ConditionExpression:      aws.String(cond),
		ExpressionAttributeNames: names,
		Item: item,
	})
	if err != nil {
		w.err = err
	} else {
		w.keys = append(w.keys, key)
	}
	return err
}

func (w *inserter) Flush(ctx context.Context) error {
	if len(w.buf) == 0 {
		return w.err
	}
	resp, err := w.db.BatchWriteItemWithContext(ctx, &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]*dynamodb.WriteRequest{
			*w.tbl.name: w.buf,
		},
	})
	if err != nil {
		w.err = err
		return err
	} else if len(resp.UnprocessedItems) != 0 {
		w.err = fmt.Errorf("%d items were not processed", len(resp.UnprocessedItems))
		return w.err
	}
	w.keys = append(w.keys, w.ikeys...)
	w.ikeys = w.ikeys[:0]
	w.buf = w.buf[:0]
	return w.err
}

func (w *inserter) Keys() []nosql.Key {
	return w.keys
}

func (w *inserter) Close() error {
	w.ikeys = nil
	w.buf = nil
	return w.err
}
