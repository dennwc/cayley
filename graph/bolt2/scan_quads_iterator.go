package bolt

import (
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/quad"
	"log"
)

func newScanQuads(s *Store, it graph.Iterator, dirs [4]quad.Direction, ids []ID) graph.Iterator {
	return &scanQuadsIterator{
		Iterator: it, s: s, dirs: dirs, ids: ids,
	}
}

type scanQuadsIterator struct {
	graph.Iterator
	s    *Store
	dirs [4]quad.Direction
	ids  []ID
	err  error
}

func (it *scanQuadsIterator) Err() error {
	if it.err != nil {
		return it.err
	}
	return it.Iterator.Err()
}
func (it *scanQuadsIterator) checkInternal(q InternalQuad) bool {
	if q == (InternalQuad{}) {
		return false
	}
	for i, d := range it.dirs {
		if d == quad.Any {
			break
		}
		if q.Get(d) != it.ids[i] {
			return false
		}
	}
	return true
}
func (it *scanQuadsIterator) Next() bool {
	//if len(it.vals) != len(it.ids) {
	//	it.vals, it.err = it.s.GetValues(it.ids)
	//}
	for it.err == nil && it.Iterator.Next() {
		v := it.Iterator.Result()
		log.Printf("scan: %T - %v", v, v)
		var ok bool
		switch v := v.(type) {
		case ID:
			q, err := it.s.GetInternalQuads([]ID{v})
			if err != nil {
				it.err = err
				return false
			}
			ok = it.checkInternal(q[0])
		case InternalQuad:
			ok = it.checkInternal(v)
		}
		if ok {
			return true
		}
	}
	return false
}
func (it *scanQuadsIterator) Contains(v graph.Value) bool {
	switch v := v.(type) {
	case ID:
		q, err := it.s.GetInternalQuads([]ID{v})
		if err != nil {
			it.err = err
			return false
		}
		if !it.checkInternal(q[0]) {
			return false
		}
		return it.Iterator.Contains(v)
	case InternalQuad:
		if !it.checkInternal(v) {
			return false
		}
		return it.Iterator.Contains(v)
	default:
		return false
	}
}
