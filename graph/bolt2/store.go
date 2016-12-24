package bolt

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/boltdb/bolt"
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/quad"
	"github.com/cayleygraph/cayley/quad/pquads"
	"github.com/cayleygraph/cayley/voc/rdf"
	"time"
)

const latestDataVersion = 4

var (
	bucketPrim      = []byte("prim")
	bucketValueHash = []byte("value-hash")
	bucketMeta      = []byte("meta")
)

var (
	idOrder = binary.LittleEndian
)

var (
	errNoBucket = errors.New("bolt: bucket is missing")
)

func clone(p []byte) []byte {
	c := make([]byte, len(p))
	copy(c, p)
	return c
}

type ID uint64

func (id ID) Bytes() []byte {
	var p [8]byte
	idOrder.PutUint64(p[:], uint64(id))
	return p[:]
}
func IDFrom(p []byte) ID {
	return ID(idOrder.Uint64(p))
}

type Options struct {
	Bolt   *bolt.Options
	NoSync bool
}

func (opt *Options) set(db *bolt.DB) {
	db.NoSync = opt.NoSync
}

func Create(path string, opts *Options) (*Store, error) {
	if opts == nil {
		opts = &Options{}
	}
	db, err := bolt.Open(path, 0644, opts.Bolt)
	if err != nil {
		return nil, err
	}
	opts.set(db)
	s := &Store{db: db}
	if err = s.getMetadata(); err == nil {
		db.Close()
		return nil, graph.ErrDatabaseExists
	} else if err != errNoBucket {
		db.Close()
		return nil, err
	}
	if err = s.createBuckets(); err != nil {
		db.Close()
		return nil, err
	}
	if err = setVersion(s.db, latestDataVersion); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

func Open(path string, opts *Options) (*Store, error) {
	if opts == nil {
		opts = &Options{}
	}
	db, err := bolt.Open(path, 0644, opts.Bolt)
	if err != nil {
		return nil, err
	}
	opts.set(db)
	s := &Store{db: db}
	if err = s.getMetadata(); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

func setVersion(db *bolt.DB, version int64) error {
	return db.Update(func(tx *bolt.Tx) error {
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, uint64(version))
		b := tx.Bucket(bucketMeta)
		return b.Put([]byte("version"), buf)
	})
}

type Store struct {
	db      *bolt.DB
	version int64
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) getMetadata() error {
	return s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketMeta)
		if b == nil {
			return errNoBucket
		}
		if v := b.Get([]byte("version")); v != nil {
			s.version = int64(binary.LittleEndian.Uint64(v))
		}
		return nil
	})
}

func (s *Store) createBuckets() error {
	return s.db.Update(func(tx *bolt.Tx) error {
		for _, name := range [][]byte{
			bucketMeta,
			bucketPrim,
			bucketValueHash,
		} {
			if _, err := tx.CreateBucket(name); err != nil {
				return fmt.Errorf("could not create bucket %q: %v", string(name), err)
			}
		}
		return nil
	})
}

func openBucket(tx *bolt.Tx, name []byte) (*bolt.Bucket, error) {
	if b := tx.Bucket(name); b != nil {
		return b, nil
	}
	return nil, bolt.ErrBucketNotFound
}
func (s *Store) appendRecord(b *bolt.Bucket, val []byte) (ID, error) {
	n, err := b.NextSequence()
	if err != nil {
		return 0, err
	}
	id := ID(n)
	if err = b.Put(id.Bytes(), val); err != nil {
		return 0, err
	}
	return id, nil
}
func (s *Store) removeRecord(b *bolt.Bucket, id ID) error {
	return b.Delete(id.Bytes())
}
func (s *Store) ResolveValues(vals []quad.Value) ([]ID, error) {
	ids := make([]ID, 0, len(vals))
	err := s.db.View(func(tx *bolt.Tx) error {
		prim, vhash := tx.Bucket(bucketPrim), tx.Bucket(bucketValueHash)
		for _, v := range vals {
			id, err := s.resolveValue(v, prim, vhash, nil)
			if err != nil {
				return err
			}
			ids = append(ids, id)
		}
		return nil
	})
	return ids, err
}
func (s *Store) GetValues(ids []ID) ([]quad.Value, error) {
	out := make([]quad.Value, len(ids))
	err := s.db.View(func(tx *bolt.Tx) error {
		b, err := openBucket(tx, bucketPrim)
		if err != nil {
			return err
		}
		var p pquads.Primitive
		for i, id := range ids {
			v := b.Get(id.Bytes())
			if v == nil {
				continue
			}
			p.Reset()
			if err := p.Unmarshal(v); err != nil {
				return err
			}
			out[i] = p.Value.ToNative()
		}
		return nil
	})
	return out, err
}
func (s *Store) GetInternalQuads(ids []ID) ([]InternalQuad, error) {
	out := make([]InternalQuad, len(ids))
	err := s.db.View(func(tx *bolt.Tx) error {
		b, err := openBucket(tx, bucketPrim)
		if err != nil {
			return err
		}
		var p pquads.Primitive
		for i, id := range ids {
			v := b.Get(id.Bytes())
			if v == nil {
				continue
			}
			p.Reset()
			if err := p.Unmarshal(v); err != nil {
				return err
			}
			out[i] = InternalQuad{
				Subject:   ID(p.Subject),
				Predicate: ID(p.Predicate),
				Object:    ID(p.Object),
				Label:     ID(p.Label),
			}
		}
		return nil
	})
	return out, err
}
func (s *Store) FillQuads(quads []InternalQuad) ([]quad.Quad, error) {
	out := make([]quad.Quad, len(quads))
	err := s.db.View(func(tx *bolt.Tx) error {
		prim := tx.Bucket(bucketPrim)
		var (
			p    pquads.Primitive
			last error
		)
		lookup := func(id ID) quad.Value {
			v := prim.Get(id.Bytes())
			if v == nil {
				return nil
			}
			p.Reset()
			if err := p.Unmarshal(v); err != nil {
				last = err
				return nil
			}
			return p.Value.ToNative()
		}
		for i, q := range quads {
			out[i] = quad.Quad{
				Subject:   lookup(q.Subject),
				Predicate: lookup(q.Predicate),
				Object:    lookup(q.Object),
				Label:     lookup(q.Label),
			}
		}
		return last
	})
	return out, err
}
func (s *Store) GetQuads(ids []ID) ([]quad.Quad, error) {
	quads, err := s.GetInternalQuads(ids)
	if err != nil {
		return nil, err
	}
	return s.FillQuads(quads)
}
func (s *Store) resolveValue(v quad.Value, prim, vhash *bolt.Bucket, h []byte) (ID, error) {
	if v == nil {
		return 0, nil
	}
	if vhash != nil { // fast path - lookup hash index
		if h == nil {
			h = quad.HashOf(v)
		}
		if sid := vhash.Get(h); len(sid) != 0 {
			// TODO(dennwc): optionally check for collisions
			return IDFrom(sid), nil
		}
		return 0, nil
	}
	// slow path - scan everything
	return 0, fmt.Errorf("nodes scan not implemented") // TODO
}
func (s *Store) indexValuePrimary(id ID, v quad.Value, vhash *bolt.Bucket, h []byte) error {
	if err := vhash.Put(h, id.Bytes()); err != nil {
		return err
	}
	return nil
}
func (s *Store) resolveOrAppendValue(v quad.Value, prim, vhash *bolt.Bucket) (ID, error) {
	if v == nil {
		return 0, nil
	}
	h := quad.HashOf(v)
	if id, err := s.resolveValue(v, prim, vhash, h); err != nil {
		return 0, err
	} else if id != 0 {
		return id, nil
	}
	p := &pquads.Primitive{
		Type:  pquads.Primitive_NODE,
		Value: pquads.MakeValue(v),
		Ts:    pquads.MakeTimestamp(time.Now()),
	}
	data, err := p.Marshal()
	if err != nil {
		return 0, err
	}
	id, err := s.appendRecord(prim, data)
	if err != nil {
		return 0, err
	}
	if err = s.indexValuePrimary(id, v, vhash, h); err != nil {
		// node is nearly unusable in this state, so we'll try to remove it
		_ = s.removeRecord(prim, id)
		return 0, err
	}
	// TODO(dennwc): optionally update secondary indexes
	return id, nil
}

func (s *Store) reindexPrimitives(typ pquads.Primitive_Type, pre func(tx *bolt.Tx) error, index func(id ID, p *pquads.Primitive) error) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b, err := openBucket(tx, bucketPrim)
		if err != nil {
			return err
		}
		if err := pre(tx); err != nil {
			return err
		}
		var (
			id ID
			p  pquads.Primitive
		)
		return b.ForEach(func(k, v []byte) error {
			p.Reset()
			// TODO(dennwc): first unmarshal type only
			if err := p.Unmarshal(v); err != nil {
				return err
			}
			if typ != pquads.Primitive_UNKNOWN && p.Type != typ {
				return nil
			}
			id = IDFrom(k)
			return index(id, &p)
		})
	})
}

func quadIndexName(ind [4]quad.Direction) []byte {
	const (
		pref = "index-"
		off  = len(pref)
	)
	name := make([]byte, off+len(ind))
	for i, d := range ind {
		name[off+i] = d.Prefix()
	}
	return name[:]
}

func (s *Store) ReindexQuads() error {
	var (
		indexes = quad.DefaultIndexes
		buckets = make([]*bolt.Bucket, 0, len(indexes))
	)
	return s.reindexPrimitives(pquads.Primitive_QUAD, func(tx *bolt.Tx) error {
		buckets = buckets[:0]
		for _, ind := range indexes {
			name := quadIndexName(ind)
			if err := tx.DeleteBucket(name); err != nil && err != bolt.ErrBucketNotFound {
				return err
			}
			b, err := tx.CreateBucket(name)
			if err != nil {
				return err
			}
			buckets = append(buckets, b)
		}
		return nil
	}, func(id ID, p *pquads.Primitive) error {
		for i, ind := range indexes {
			b := buckets[i]
			q := InternalQuad{
				Subject:   ID(p.Subject),
				Predicate: ID(p.Predicate),
				Object:    ID(p.Object),
				Label:     ID(p.Label),
			}
			buf := q.Bytes(ind)
			if err := b.Put(buf[:], id.Bytes()); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Store) QuadBatchWriter() *QuadBatchWriter {
	w := &QuadBatchWriter{
		s: s,
	}
	w.Insert()
	return w
}

var (
	_ quad.WriteCloser = (*QuadBatchWriter)(nil)
	_ quad.BatchWriter = (*QuadBatchWriter)(nil)
)

type QuadBatchWriter struct {
	s      *Store
	sameAs ID

	// function for quad insertions; can be nil, meaning to skip quad insertion
	writeQuad func(InternalQuad) error

	// valid only inside tx
	prim, primHash *bolt.Bucket

	// cache last seen values, it will save few lookups for sorted quad sets
	ps, pp, po, pl resolvedValue
}

func (w *QuadBatchWriter) WriteQuadFunc(fnc func(InternalQuad) error) {
	w.writeQuad = fnc
}

// Insert switches writer to quad add/insert mode.
func (w *QuadBatchWriter) Insert() {
	w.writeQuad = w.appendQuad
}

// Delete switches writer to quad delete mode.
func (w *QuadBatchWriter) Delete() {
	panic("not implemented")
}
func (w *QuadBatchWriter) Close() error {
	return nil
}

type resolvedValue struct {
	ID    ID
	Value quad.Value
}

func (w *QuadBatchWriter) resolveOrAppendValue(v quad.Value, prev *resolvedValue) (ID, error) {
	if v == nil {
		return 0, nil
	}
	if prev != nil && v == prev.Value {
		return prev.ID, nil
	}
	id, err := w.s.resolveOrAppendValue(v, w.prim, w.primHash)
	if err != nil {
		return 0, err
	}
	if iri, ok := v.(quad.IRI); ok {
		switch string(iri.Short()) {
		case rdf.SameAs:
			w.sameAs = id
		}
	}
	if prev != nil {
		*prev = resolvedValue{ID: id, Value: v}
	}
	return id, nil
}
func (w *QuadBatchWriter) appendQuad(q InternalQuad) error {
	// handle special predicates
	switch q.Predicate {
	case w.sameAs:
		// find Subject and add appropriate field to record
		// FIXME(dennwc): we might overwrite sameAs if this quad has label
		sid := q.Subject.Bytes()

		data := w.prim.Get(sid)
		if len(data) == 0 {
			return errors.New("subject for sameAs not found")
		}
		var pr pquads.Primitive
		if err := pr.Unmarshal(data); err != nil {
			return err
		}
		pr.SameAs = uint64(q.Object)
		data, err := pr.Marshal()
		if err != nil {
			return err
		}
		if err = w.prim.Put(sid, data); err != nil {
			return err
		}
	}
	// TODO(dennwc): optionally check for duplicates
	pr := &pquads.Primitive{
		Type:      pquads.Primitive_QUAD,
		Subject:   uint64(q.Subject),
		Predicate: uint64(q.Predicate),
		Object:    uint64(q.Object),
		Label:     uint64(q.Label),
		Ts:        pquads.MakeTimestamp(time.Now()),
	}
	data, err := pr.Marshal()
	if err != nil {
		return err
	}
	_, err = w.s.appendRecord(w.prim, data)
	if err != nil {
		return err
	}
	return nil
}
func (w *QuadBatchWriter) removeQuad(q InternalQuad) error {
	// handle special predicates
	switch q.Predicate {
	case w.sameAs:
		// find Subject and add appropriate field to record
		// FIXME(dennwc): we might overwrite sameAs if this quad has label
		sid := q.Subject.Bytes()

		data := w.prim.Get(sid)
		if len(data) == 0 {
			return errors.New("subject for sameAs not found")
		}
		var pr pquads.Primitive
		if err := pr.Unmarshal(data); err != nil {
			return err
		}
		pr.SameAs = uint64(q.Object)
		data, err := pr.Marshal()
		if err != nil {
			return err
		}
		if err = w.prim.Put(sid, data); err != nil {
			return err
		}
	}
	// TODO(dennwc): optionally check for duplicates
	pr := &pquads.Primitive{
		Type:      pquads.Primitive_QUAD,
		Subject:   uint64(q.Subject),
		Predicate: uint64(q.Predicate),
		Object:    uint64(q.Object),
		Label:     uint64(q.Label),
		Ts:        pquads.MakeTimestamp(time.Now()),
	}
	data, err := pr.Marshal()
	if err != nil {
		return err
	}
	_, err = w.s.appendRecord(w.prim, data)
	if err != nil {
		return err
	}
	return nil
}

type InternalQuad struct {
	Subject   ID
	Predicate ID
	Object    ID
	Label     ID
}

func (q InternalQuad) Get(dir quad.Direction) ID {
	switch dir {
	case quad.Subject:
		return q.Subject
	case quad.Predicate:
		return q.Predicate
	case quad.Object:
		return q.Object
	case quad.Label:
		return q.Label
	default:
		panic("unknown quad direction")
	}
}

type InternalQuadBuf [4 * 8]byte

func (q InternalQuad) Bytes(dir [4]quad.Direction) InternalQuadBuf {
	var p InternalQuadBuf
	q.PutTo(dir, p[:])
	return p
}
func (q InternalQuad) PutTo(dir [4]quad.Direction, p []byte) {
	if len(p) < 4*8 {
		panic("short buffer")
	}
	if dir == ([4]quad.Direction{}) {
		dir = [4]quad.Direction{quad.Subject, quad.Predicate, quad.Object, quad.Label}
	}
	for i, d := range dir {
		idOrder.PutUint64(p[i*8:], uint64(q.Get(d)))
	}
}

func (w *QuadBatchWriter) update(fnc func() error) error {
	return w.s.db.Update(func(tx *bolt.Tx) error {
		var err error
		w.prim, err = openBucket(tx, bucketPrim)
		if err != nil {
			return err
		}
		w.primHash, err = openBucket(tx, bucketValueHash)
		if err != nil {
			return err
		}
		return fnc()
	})
}
func (w *QuadBatchWriter) WriteInternalQuads(quads []InternalQuad) (int, error) {
	var n int
	err := w.update(func() error {
		for _, q := range quads {
			if err := w.appendQuad(q); err != nil {
				return err
			}
			n++
		}
		return nil
	})
	return n, err
}
func (w *QuadBatchWriter) WriteQuad(q quad.Quad) error {
	_, err := w.WriteQuads([]quad.Quad{q})
	return err
}
func (w *QuadBatchWriter) WriteQuads(quads []quad.Quad) (int, error) {
	var n int
	err := w.update(func() error {
		for _, q := range quads {
			s, err := w.resolveOrAppendValue(q.Subject, &w.ps)
			if err != nil {
				return err
			}
			p, err := w.resolveOrAppendValue(q.Predicate, &w.pp)
			if err != nil {
				return err
			}
			o, err := w.resolveOrAppendValue(q.Object, &w.po)
			if err != nil {
				return err
			}
			l, err := w.resolveOrAppendValue(q.Label, &w.pl)
			if err != nil {
				return err
			}
			if w.writeQuad != nil {
				if err = w.writeQuad(InternalQuad{
					Subject:   s,
					Predicate: p,
					Object:    o,
					Label:     l,
				}); err != nil {
					return err
				}
			}
			n++
		}
		return nil
	})
	return n, err
}

func (s *Store) QuadIterator(dirs [4]quad.Direction, vals []ID) graph.Iterator {
	// FIXME
	return newScanQuads(s, &allIterator{
		uid: iterator.NextUID(),
		it:  s.newPrimIterator(pquads.Primitive_QUAD, false),
	}, dirs, vals)
}

func (s *Store) PrimitivesIterator(typ pquads.Primitive_Type) graph.Iterator {
	panic("not implemented")
}
