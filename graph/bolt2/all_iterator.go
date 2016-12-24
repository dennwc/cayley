package bolt

import (
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/iterator"
)

var _ graph.Iterator = (*allIterator)(nil)

type allIterator struct {
	uid  uint64
	tags graph.Tagger
	it   *primIterator
}

func (it *allIterator) UID() uint64           { return it.uid }
func (it *allIterator) Tagger() *graph.Tagger { return &it.tags }
func (it *allIterator) TagResults(dst map[string]graph.Value) {
	for _, tag := range it.tags.Tags() {
		dst[tag] = it.Result()
	}
	for tag, value := range it.tags.Fixed() {
		dst[tag] = value
	}
}
func (it *allIterator) Result() graph.Value {
	k := it.it.Key()
	if k == 0 {
		return nil
	}
	return k
}
func (it *allIterator) Next() bool     { return it.it.Next() }
func (it *allIterator) NextPath() bool { return false }
func (it *allIterator) Contains(v graph.Value) bool {
	switch v := v.(type) {
	case ID:
		return it.it.Seek(v)
		// TODO(dennwc): handle quads
	default:
		return false
	}
}
func (it *allIterator) Err() error { return it.it.Err() }
func (it *allIterator) Reset()     { it.it.Reset() }
func (it *allIterator) Clone() graph.Iterator {
	out := &allIterator{
		uid: iterator.NextUID(),
		it:  it.it.Clone(),
	}
	out.tags.CopyFrom(it)
	return out
}
func (it *allIterator) Stats() graph.IteratorStats {
	s, exact := it.Size()
	return graph.IteratorStats{
		ContainsCost: 1,
		NextCost:     2,
		Size:         s,
		ExactSize:    exact,
	}
}
func (it *allIterator) Size() (int64, bool) {
	return 0, false // FIXME
}
func (it *allIterator) Type() graph.Type {
	return graph.All
}
func (it *allIterator) Optimize() (graph.Iterator, bool) {
	return it, false
}
func (it *allIterator) SubIterators() []graph.Iterator {
	return nil
}
func (it *allIterator) Describe() graph.Description {
	size, _ := it.Size()
	return graph.Description{
		UID:  it.UID(),
		Type: it.Type(),
		Tags: it.tags.Tags(),
		Size: size,
	}
}
func (it *allIterator) Close() error {
	return it.it.Close()
}
