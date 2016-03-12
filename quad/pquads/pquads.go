package pquads

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"github.com/google/cayley/graph/proto"
	"github.com/google/cayley/quad"
	"io"
)

func NewWriter(w io.Writer) *Writer {
	return &Writer{
		w: w,
	}
}

type Writer struct {
	w   io.Writer
	buf []byte
}

func (w *Writer) WriteQuad(q quad.Quad) error {
	pq := proto.MakeQuad(q)
	sz := pq.ProtoSize()
	if n := sz + binary.MaxVarintLen64; len(w.buf) < n {
		w.buf = make([]byte, n)
	}
	n := binary.PutVarint(w.buf, int64(sz))
	if qn, err := pq.MarshalTo(w.buf[n:]); err != nil {
		return err
	} else if qn != sz {
		panic(fmt.Errorf("unexpected size of proto message: %v vs %v", qn, sz))
	}
	_, err := w.w.Write(w.buf[:n+sz])
	return err
}

func NewReader(r io.Reader) *Reader {
	if br, ok := r.(io.ByteReader); ok {
		return &Reader{
			r: r, br: br,
		}
	}
	br := bufio.NewReader(r)
	return &Reader{
		r: br, br: br,
	}
}

type Reader struct {
	r   io.Reader
	br  io.ByteReader
	buf []byte
}

func (r *Reader) ReadQuad() (quad.Quad, error) {
	sz, err := binary.ReadVarint(r.br)
	if err != nil {
		return quad.Quad{}, err
	}
	if len(r.buf) < int(sz) {
		r.buf = make([]byte, sz)
	}
	_, err = io.ReadFull(r.r, r.buf[:sz])
	if err != nil {
		return quad.Quad{}, err
	}
	var pq proto.Quad
	if err = pq.Unmarshal(r.buf[:sz]); err != nil {
		return quad.Quad{}, err
	}
	return pq.ToNative(), nil
}
