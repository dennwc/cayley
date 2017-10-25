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
	"github.com/pborman/uuid"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

const Type = "dynamo"

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
	return &DB{pref: addr + "_", db: dynamodb.New(sess)}, nil
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

type DB struct {
	pref string
	db   *dynamodb.DynamoDB
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

func (db *DB) EnsureIndex(col string, indexes ...nosql.Index) error {
	tbl := db.col(col)
	_, err := db.db.CreateTable(&dynamodb.CreateTableInput{
		TableName: tbl,
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String(nosql.IDField),
				AttributeType: aws.String("S"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String(nosql.IDField),
				KeyType:       aws.String("HASH"),
			},
		},
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
	return db.waitTable(context.TODO(), tbl)
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
	case nosql.Document:
		_, ok1 := v[fldType]
		_, ok2 := v[fldVal]
		if ok1 || ok2 {
			panic(fmt.Errorf("document collides with internal types: %v", v))
		}
		return &dynamodb.AttributeValue{M: toAwsDoc(v)}
	case nosql.Array:
		arr := make([]*dynamodb.AttributeValue, 0, len(v))
		for _, s := range v {
			arr = append(arr, toAwsValue(s))
		}
		return &dynamodb.AttributeValue{L: arr}
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
	case v.L != nil:
		arr := make(nosql.Array, 0, len(v.L))
		for _, s := range v.L {
			arr = append(arr, fromAwsValue(s))
		}
		return arr
	case v.SS != nil:
		arr := make(nosql.Array, 0, len(v.SS))
		for _, s := range v.SS {
			arr = append(arr, nosql.String(*s))
		}
		return arr
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

func genID() string {
	return uuid.NewUUID().String()
}

func (db *DB) Insert(col string, id string, d nosql.Document) (string, error) {
	if id == "" {
		id = genID()
	}
	item := toAwsDoc(d)
	item[nosql.IDField] = str(id)
	_, err := db.db.PutItem(&dynamodb.PutItemInput{
		TableName:           db.col(col),
		ConditionExpression: aws.String("attribute_not_exists(#id)"),
		ExpressionAttributeNames: map[string]*string{
			"#id": aws.String(nosql.IDField),
		},
		Item: item,
	})
	if err != nil {
		return "", err
	}
	return id, nil
}
func (db *DB) FindByID(col string, id string) (nosql.Document, error) {
	resp, err := db.db.GetItem(&dynamodb.GetItemInput{
		TableName: db.col(col),
		Key: map[string]*dynamodb.AttributeValue{
			nosql.IDField: str(id),
		},
	})
	if err != nil {
		return nil, convError(err)
	} else if len(resp.Item) == 0 {
		return nil, nosql.ErrNotFound
	}
	return fromAwsDoc(resp.Item), nil
}
func (db *DB) Query(col string) nosql.Query {
	return &Query{db: db.db, col: db.col(col), f: newFilter()}
}
func (db *DB) Update(col string, id string) nosql.Update {
	return &Update{
		db: db.db, col: db.col(col), id: id,
		names: make(map[string]*string),
		vals:  make(map[string]*dynamodb.AttributeValue),
	}
}
func (db *DB) Delete(col string) nosql.Delete {
	return &Delete{db: db.db, col: db.col(col), f: newFilter()}
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
	col   *string
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
		TableName:                 q.col,
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
	return fromAwsDoc(resp.Items[0]), nil
}
func (q *Query) Iterate() nosql.DocIterator {
	qu := q.build()
	return &Iterator{db: q.db, q: qu}
}

type Iterator struct {
	db *dynamodb.DynamoDB
	q  *dynamodb.ScanInput

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
func (it *Iterator) ID() string {
	f := it.item()[nosql.IDField]
	if f != nil && f.S != nil {
		return string(*f.S)
	}
	return ""
}
func (it *Iterator) Doc() nosql.Document {
	return fromAwsDoc(it.item())
}

type Delete struct {
	db  *dynamodb.DynamoDB
	col *string
	ids []string
	f   Filter
}

func (d *Delete) WithFields(filters ...nosql.FieldFilter) nosql.Delete {
	d.f.Append(filters)
	return d
}
func (d *Delete) IDs(ids ...string) nosql.Delete {
	d.ids = append(d.ids, ids...)
	return d
}
func (d *Delete) Do(ctx context.Context) error {
	if len(d.ids) == 0 {
		return fmt.Errorf("expected one or more object ids")
	}
	req := dynamodb.DeleteItemInput{
		TableName:                 d.col,
		ConditionExpression:       d.f.Expr(),
		ExpressionAttributeNames:  d.f.Names(),
		ExpressionAttributeValues: d.f.Values(),
	}
	for _, id := range d.ids {
		r := req
		r.Key = map[string]*dynamodb.AttributeValue{
			nosql.IDField: str(id),
		}
		_, err := d.db.DeleteItem(&r)
		if e, ok := err.(awserr.Error); ok {
			switch e.Code() {
			case "ConditionalCheckFailedException":
				err = nil
			}
		}
		if err != nil {
			return err
		}
	}
	return nil
}

type Update struct {
	db  *dynamodb.DynamoDB
	col *string
	id  string

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
	delete(upsert, nosql.IDField)
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
		TableName: u.col,
		Key: map[string]*dynamodb.AttributeValue{
			nosql.IDField: str(u.id),
		},
	}
	if len(u.names) != 0 {
		req.ExpressionAttributeNames = u.names
		req.ExpressionAttributeValues = u.vals
	}
	if !u.upsert {
		req.ConditionExpression = aws.String("attribute_not_exists(" + nosql.IDField + ")")
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
