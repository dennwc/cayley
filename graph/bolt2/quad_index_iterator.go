package bolt

/*
import (
	"bytes"
	"errors"
	"fmt"

	"github.com/boltdb/bolt"

	"github.com/cayleygraph/cayley/clog"
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/iterator"
)

var _ graph.Iterator = (*indexIterator)(nil)

var (
	boltType    graph.Type
	bufferSize  = 50
	errNotExist = errors.New("quad does not exist")
)

func init() {
	boltType = graph.RegisterIterator(Type)
}

type indexIterator struct {
	uid     uint64
	tags    graph.Tagger
	s *Store
	err     error
	it *Iterator
}

func (s *Store) NewIndexIterator(bucket []byte, vals []ID) *indexIterator {
	tok := value.(*Token)
	if !bytes.Equal(tok.bucket, nodeBucket) {
		clog.Errorf("creating an iterator from a non-node value")
		return &Iterator{done: true}
	}

	it := Iterator{
		uid:    iterator.NextUID(),
		bucket: bucket,
		dir:    d,
		qs:     qs,
		size:   qs.SizeOf(value),
	}

	it.checkID = make([]byte, len(tok.key))
	copy(it.checkID, tok.key)

	return &it
}

func IteratorType() graph.Type { return boltType }

func (it *indexIterator) UID() uint64 {
	return it.uid
}

func (it *indexIterator) Reset() {
	it.keys = nil
	it.offset = 0
	it.done = false
}

func (it *indexIterator) Tagger() *graph.Tagger {
	return &it.tags
}

func (it *indexIterator) TagResults(dst map[string]graph.Value) {
	for _, tag := range it.tags.Tags() {
		dst[tag] = it.Result()
	}

	for tag, value := range it.tags.Fixed() {
		dst[tag] = value
	}
}

func (it *indexIterator) Clone() graph.Iterator {
	out := NewIterator(it.bucket, it.dir, &Token{true, nodeBucket, it.checkID}, it.qs)
	out.Tagger().CopyFrom(it)
	return out
}

func (it *indexIterator) Close() error {
	it.result = nil
	it.keys = nil
	it.done = true
	return nil
}

func (it *indexIterator) Next() bool {
	if it.done {
		return false
	}
	if len(it.keys) <= it.offset+1 {
		it.offset = 0
		var last []byte
		if it.keys != nil {
			last = it.keys[len(it.keys)-1]
		}
		it.keys = make([][]byte, 0, bufferSize)
		err := it.qs.db.View(func(tx *bolt.Tx) error {
			i := 0
			b := tx.Bucket(it.bucket)
			cur := b.Cursor()
			if last == nil {
				k, v := cur.Seek(it.checkID)
				if bytes.HasPrefix(k, it.checkID) {
					if isLiveValue(v) {
						it.keys = append(it.keys, clone(k))
						i++
					}
				} else {
					it.keys = append(it.keys, nil)
					return errNotExist
				}
			} else {
				k, _ := cur.Seek(last)
				if !bytes.Equal(k, last) {
					return fmt.Errorf("could not pick up after %v", k)
				}
			}
			for i < bufferSize {
				k, v := cur.Next()
				if k == nil || !bytes.HasPrefix(k, it.checkID) {
					it.keys = append(it.keys, nil)
					break
				}
				if !isLiveValue(v) {
					continue
				}
				it.keys = append(it.keys, clone(k))
				i++
			}
			return nil
		})
		if err != nil {
			if err != errNotExist {
				clog.Errorf("Error nexting in database: %v", err)
				it.err = err
			}
			it.done = true
			return false
		}
	} else {
		it.offset++
	}
	if it.Result() == nil {
		it.done = true
		return false
	}
	return true
}

func (it *indexIterator) Err() error {
	return it.err
}

func (it *indexIterator) Result() graph.Value {
	if it.done {
		return nil
	}
	if it.result != nil {
		return it.result
	}
	if it.offset >= len(it.keys) {
		return nil
	}
	if it.keys[it.offset] == nil {
		return nil
	}
	return &Token{bucket: it.bucket, key: it.keys[it.offset]}
}

func (it *indexIterator) NextPath() bool {
	return false
}

// No subiterators.
func (it *indexIterator) SubIterators() []graph.Iterator {
	return nil
}

func (it *indexIterator) Contains(v graph.Value) bool {
	val := v.(*Token)
	if bytes.Equal(val.bucket, nodeBucket) {
		return false
	}
	offset := PositionOf(val, it.dir, it.qs)
	if len(val.key) != 0 && bytes.HasPrefix(val.key[offset:], it.checkID) {
		// You may ask, why don't we check to see if it's a valid (not deleted) quad
		// again?
		//
		// We've already done that -- in order to get the graph.Value token in the
		// first place, we had to have done the check already; it came from a Next().
		//
		// However, if it ever starts coming from somewhere else, it'll be more
		// efficient to change the interface of the graph.Value for LevelDB to a
		// struct with a flag for isValid, to save another random read.
		return true
	}
	return false
}

func (it *indexIterator) Size() (int64, bool) {
	return it.size, true
}

func (it *indexIterator) Describe() graph.Description {
	return graph.Description{
		UID:       it.UID(),
		Name:      it.qs.NameOf(&Token{true, it.bucket, it.checkID}).String(),
		Type:      it.Type(),
		Tags:      it.tags.Tags(),
		Size:      it.size,
		Direction: it.dir,
	}
}

func (it *indexIterator) Type() graph.Type { return boltType }
func (it *indexIterator) Sorted() bool     { return false }

func (it *indexIterator) Optimize() (graph.Iterator, bool) {
	return it, false
}

func (it *indexIterator) Stats() graph.IteratorStats {
	s, exact := it.Size()
	return graph.IteratorStats{
		ContainsCost: 1,
		NextCost:     4,
		Size:         s,
		ExactSize:    exact,
	}
}
*/
