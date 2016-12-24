package bolt

import (
	"bytes"
	"github.com/boltdb/bolt"
)

func NewIterator(db *bolt.DB, bucket []byte) *Iterator {
	return &Iterator{
		db: db, bucket: bucket,
	}
}

type Iterator struct {
	db     *bolt.DB
	bucket []byte

	from, to []byte

	values bool
	off    int
	keys   [][]byte
	vals   [][]byte

	err  error
	done bool
}

func (it *Iterator) Err() error { return it.err }
func (it *Iterator) Clone() *Iterator {
	out := &Iterator{
		db:     it.db,
		bucket: it.bucket,
		from:   it.from, to: it.to,
		values: it.values,
	}
	if it.off < len(it.keys) {
		k := clone(it.keys[it.off])
		out.keys = [][]byte{k}
	}
	return out
}
func (it *Iterator) Range(from, to []byte) {
	it.from, it.to = from, to
}
func (it *Iterator) WithValues(v bool) {
	it.values = v
	it.vals = nil
}
func (it *Iterator) Reset() {
	it.done, it.err = false, nil
	it.keys, it.off = nil, 0
}
func (it *Iterator) Next() bool {
	if it.done || it.err != nil {
		return false
	}
	it.off++
	if it.off < len(it.keys) {
		return true
	}
	prev, skip := it.from, false
	if n := len(it.keys); n != 0 {
		prev, skip = it.keys[n-1], true
	}
	it.off = 0
	it.keys = it.keys[:0]
	it.vals = it.vals[:0]
	const buffer = 50
	err := it.db.View(func(tx *bolt.Tx) error {
		b, err := openBucket(tx, it.bucket)
		if err != nil {
			return err
		}
		c := b.Cursor()
		var k, v []byte
		if prev == nil {
			k, v = c.First()
		} else {
			k, v = c.Seek(prev)
			if skip && bytes.Equal(k, prev) {
				k, v = c.Next()
			}
		}
		for ; k != nil && len(it.keys) < buffer; k, _ = c.Next() {
			if it.to != nil && bytes.Compare(k, it.to) >= 0 {
				break
			}
			it.keys = append(it.keys, clone(k))
			if it.values {
				it.vals = append(it.vals, clone(v))
			}
		}
		return nil
	})
	if err != nil {
		it.err = err
		return false
	}
	it.done = len(it.keys) == 0
	return !it.done
}
func (it *Iterator) Key() []byte {
	if it.done || it.off >= len(it.keys) {
		return nil
	}
	return it.keys[it.off]
}
func (it *Iterator) Value() (val []byte) {
	if it.done {
		return nil
	}
	if it.off < len(it.vals) {
		return it.vals[it.off]
	}
	err := it.db.View(func(tx *bolt.Tx) error {
		b, err := openBucket(tx, it.bucket)
		if err != nil {
			return err
		}
		if v := b.Get(it.Key()); v != nil {
			val = clone(v)
		}
		return nil
	})
	if err != nil {
		it.err = err
	}
	return
}
func (it *Iterator) Seek(k []byte) (ok bool) {
	it.keys, it.vals = nil, nil
	err := it.db.View(func(tx *bolt.Tx) error {
		b, err := openBucket(tx, it.bucket)
		if err != nil {
			return err
		}
		v := b.Get(k)
		ok = v != nil
		if !ok {
			return nil
		}
		it.keys = [][]byte{k}
		if it.values {
			it.vals = [][]byte{clone(v)}
		}
		return nil
	})
	if err != nil {
		it.err = err
	}
	return
}
func (it *Iterator) Close() error {
	it.keys = nil
	it.vals = nil
	return it.err
}
