package bolt

import (
	"github.com/cayleygraph/cayley/quad/pquads"
	"log"
)

func (s *Store) newPrimIterator(typ pquads.Primitive_Type, values bool) *primIterator {
	// TODO(dennwc): type index
	it := NewIterator(s.db, bucketPrim)
	it.WithValues(values || typ != pquads.Primitive_UNKNOWN)
	return &primIterator{
		Iterator: it,
		typ:      typ,
	}
}

type primIterator struct {
	*Iterator
	typ pquads.Primitive_Type
	p   *pquads.Primitive
}

func (it *primIterator) Clone() *primIterator {
	return &primIterator{
		Iterator: it.Iterator.Clone(),
		typ:      it.typ,
	}
}
func (it *primIterator) Next() bool {
	it.p = nil
	if it.typ == pquads.Primitive_UNKNOWN {
		return it.Iterator.Next()
	}
	var p pquads.Primitive
	for it.Iterator.Next() {
		p.Reset()
		err := p.Unmarshal(it.Iterator.Value())
		if err != nil {
			it.err = err
			break
		}
		if p.Type == it.typ {
			it.p = &p
			return true
		}
		log.Printf("%+v", p)
	}
	return false
}
func (it *primIterator) Key() ID {
	k := it.Iterator.Key()
	if k == nil {
		return 0
	}
	return IDFrom(k)
}
func (it *primIterator) Value() *pquads.Primitive {
	if it.Iterator.Key() == nil {
		return nil
	} else if it.p != nil {
		return it.p
	}
	var p pquads.Primitive
	err := p.Unmarshal(it.Iterator.Value())
	if err != nil {
		it.err = err
		return nil
	}
	it.p = &p
	return it.p
}
func (it *primIterator) Seek(id ID) (ok bool) {
	it.p = nil
	return it.Iterator.Seek(id.Bytes())
}
