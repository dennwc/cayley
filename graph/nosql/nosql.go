package nosql

import (
	"context"
	"errors"
	"time"
)

const (
	IDField = "id"
)

var (
	ErrNotFound = errors.New("not found")
)

type Database interface {
	Insert(col string, id string, d Document) (string, error)
	FindByID(col string, id string) (Document, error)
	Query(col string) Query
	Update(col string, id string) Update
	Delete(col string) Delete
	// EnsureIndex
	//
	// Should create collection if it not exists
	EnsureIndex(col string, indexes ...Index) error
	Close() error
}

type Filter int

const (
	Equal = Filter(iota)
	NotEqual
	GT
	GTE
	LT
	LTE
)

type FieldFilter struct {
	Path   []string
	Filter Filter
	Value  Value
}

type Query interface {
	WithFields(filters ...FieldFilter) Query
	Limit(n int) Query

	Count(ctx context.Context) (int64, error)
	One(ctx context.Context) (Document, error)
	Iterate() DocIterator
}

type Update interface {
	//Set(d Document) Update
	Inc(field string, dn int) Update
	Push(field string, v Value) Update
	Upsert(d Document) Update
	Do(ctx context.Context) error
}

type Delete interface {
	WithFields(filters ...FieldFilter) Delete
	IDs(ids ...string) Delete
	Do(ctx context.Context) error
}

type DocIterator interface {
	Next(ctx context.Context) bool
	Err() error
	Close() error
	ID() string
	Doc() Document
}

type IndexType int

const (
	IndexAny = IndexType(iota)
	StringExact
	StringFulltext
	IntIndex
	FloatIndex
	TimeIndex
)

type Index struct {
	Field string
	Type  IndexType
}

type Value interface {
	isValue()
}

type Document map[string]Value

func (Document) isValue() {}

type String string

func (String) isValue() {}

type Int int64

func (Int) isValue() {}

type Float float64

func (Float) isValue() {}

type Bool bool

func (Bool) isValue() {}

type Time time.Time

func (Time) isValue() {}

type Bytes []byte

func (Bytes) isValue() {}

type Array []Value

func (Array) isValue() {}
