package bolt

import (
	"fmt"
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/quad"
	"github.com/cayleygraph/cayley/quad/pquads"
	"io"
	"sync/atomic"
)

const Type = "bolt2" // TODO(dennwc): change once done

var (
	_ graph.QuadStore = (*QuadStore)(nil)
	_ graph.Value     = ID(0)
	_ graph.Value     = InternalQuad{}
)

func NewQuadStore(s *Store) *QuadStore {
	return &QuadStore{s: s}
}

func (ID) IsNode() bool {
	return true // dunno
}

func (InternalQuad) IsNode() bool {
	return false
}

type QuadStore struct {
	size int64
	s    *Store
}

func (qs *QuadStore) Store() *Store { return qs.s }

type deltaReader struct {
	deltas []graph.Delta
}

func (r *deltaReader) ReadQuad() (quad.Quad, error) {
	if len(r.deltas) == 0 {
		return quad.Quad{}, io.EOF
	}
	q := r.deltas[0].Quad
	r.deltas = r.deltas[1:]
	return q, nil
}
func (qs *QuadStore) ApplyDeltas(deltas []graph.Delta, ignoreOpts graph.IgnoreOpts) error {
	allAdd := true
	for _, d := range deltas {
		if d.Action != graph.Add {
			allAdd = false
			break
		}
	}
	w := qs.s.QuadBatchWriter()
	if allAdd {
		r := &deltaReader{deltas: deltas}
		n, err := quad.CopyBatch(w, r, 0)
		if err != nil {
			w.Close()
			return err
		}
		atomic.AddInt64(&qs.size, int64(n))
		return w.Close()
	}
	mode := graph.Add
	for _, d := range deltas {
		if mode != d.Action {
			switch d.Action {
			case graph.Add:
				w.Insert()
			case graph.Delete:
				w.Delete()
			default:
				panic(fmt.Errorf("unknown action: %v", d.Action))
			}
			mode = d.Action
		}
		if err := w.WriteQuad(d.Quad); err != nil {
			w.Close()
			return err
		}
		if mode == graph.Add {
			atomic.AddInt64(&qs.size, 1)
		}
	}
	return w.Close()
}
func (qs *QuadStore) Quad(v graph.Value) quad.Quad {
	switch v := v.(type) {
	case ID:
		quads, err := qs.s.GetQuads([]ID{v})
		if err != nil || len(quads) == 0 {
			return quad.Quad{}
		}
		return quads[0]
	case InternalQuad:
		quads, err := qs.s.FillQuads([]InternalQuad{v})
		if err != nil || len(quads) == 0 {
			return quad.Quad{}
		}
		return quads[0]
	default:
		return quad.Quad{}
	}
}
func (qs *QuadStore) QuadDirection(id graph.Value, d quad.Direction) graph.Value {
	q, ok := id.(InternalQuad)
	if ok {
		return q.Get(d)
	}
	return qs.ValueOf(qs.Quad(id).Get(d))
}
func (qs *QuadStore) ValueOf(v quad.Value) graph.Value {
	ids, err := qs.s.ResolveValues([]quad.Value{v})
	if err != nil || len(ids) == 0 {
		return nil
	}
	return ids[0]
}
func (qs *QuadStore) NameOf(v graph.Value) quad.Value {
	id, ok := v.(ID)
	if !ok {
		return nil
	}
	values, err := qs.s.GetValues([]ID{id})
	if err != nil || len(values) == 0 {
		return nil
	}
	return values[0]
}

func (qs *QuadStore) Size() int64 {
	return atomic.LoadInt64(&qs.size)
}
func (qs *QuadStore) Horizon() graph.PrimaryKey {
	// FIXME(dennwc)
	return graph.NewSequentialKey(0)
}

func (qs *QuadStore) Close() error {
	return qs.s.Close()
}
func (qs *QuadStore) Type() string {
	return Type
}
func (qs *QuadStore) QuadIterator(dir quad.Direction, v graph.Value) graph.Iterator {
	id, ok := v.(ID)
	if !ok {
		return iterator.NewNull()
	}
	index := [4]quad.Direction{dir, quad.Any, quad.Any, quad.Any}
	return qs.s.QuadIterator(index, []ID{id})
}
func (qs *QuadStore) newAllIterator(typ pquads.Primitive_Type) *allIterator {
	return &allIterator{
		uid: iterator.NextUID(),
		it:  qs.s.newPrimIterator(typ, false),
	}
}

// Returns an iterator enumerating all nodes in the graph.
func (qs *QuadStore) NodesAllIterator() graph.Iterator {
	return qs.newAllIterator(pquads.Primitive_NODE)
}

// Returns an iterator enumerating all links in the graph.
func (qs *QuadStore) QuadsAllIterator() graph.Iterator {
	return qs.newAllIterator(pquads.Primitive_QUAD)
}
func (qs *QuadStore) FixedIterator() graph.FixedIterator {
	return iterator.NewFixed(iterator.Identity)
}
func (qs *QuadStore) OptimizeIterator(it graph.Iterator) (graph.Iterator, bool) {
	return it, false // TODO(dennwc)
}
