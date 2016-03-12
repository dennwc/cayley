// Package jsonld provides an encoder/decoder for JSON-LD quad format
package jsonld

import (
	"encoding/json"
	"github.com/google/cayley/quad"
	"github.com/google/cayley/quad/turtle"
	"github.com/linkeddata/gojsonld"
	"io"
)

func init() {
	quad.RegisterFormat(quad.Format{
		Name:   "jsonld",
		Ext:    []string{".jsonld"},
		Mime:   []string{"application/ld+json"},
		Writer: func(w io.Writer) quad.WriteCloser { return NewWriter(w) },
		Reader: func(r io.Reader) quad.ReadCloser { return NewReader(r) },
	})
}

// NewReader returns quad reader for JSON-LD stream.
func NewReader(r io.Reader) *Reader {
	var o interface{}
	if err := json.NewDecoder(r).Decode(&o); err != nil {
		return &Reader{err: err}
	}
	return NewReaderFromMap(o)
}

// NewReaderFromMap returns quad reader for JSON-LD map object.
func NewReaderFromMap(o interface{}) *Reader {
	data, err := gojsonld.ToRDF(o, gojsonld.NewOptions(""))
	if err != nil {
		return &Reader{err: err}
	}
	return &Reader{
		graphs: data.Graphs,
	}
}

type Reader struct {
	err    error
	name   string
	n      int
	graphs map[string][]*gojsonld.Triple
}

func (r *Reader) ReadQuad() (quad.Quad, error) {
	if r.err != nil {
		return quad.Quad{}, r.err
	}
next:
	if len(r.graphs) == 0 {
		return quad.Quad{}, io.EOF
	}
	if r.name == "" {
		for gname, _ := range r.graphs {
			r.name = gname
			break
		}
	}
	if r.n >= len(r.graphs[r.name]) {
		r.n = 0
		delete(r.graphs, r.name)
		r.name = ""
		goto next
	}
	cur := r.graphs[r.name][r.n]
	r.n++
	graph := r.name
	if graph == "@default" {
		graph = ""
	}
	return quad.Quad{
		Subject:   cur.Subject.String(),
		Predicate: cur.Predicate.String(),
		Object:    cur.Object.String(),
		Label:     graph,
	}, nil
}

func (r *Reader) Close() error {
	r.graphs = nil
	return r.err
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{w: w, ds: gojsonld.NewDataset()}
}

type Writer struct {
	w   io.Writer
	ds  *gojsonld.Dataset
	ctx interface{}
}

func (w *Writer) SetLdContext(ctx interface{}) {
	w.ctx = ctx
}

func (w *Writer) WriteQuad(q quad.Quad) error {
	graph := q.Label
	if graph == "" {
		graph = "@default"
	}
	g := w.ds.Graphs[graph]
	g = append(g, gojsonld.NewTriple(
		parseTerm(q.Subject),
		parseTerm(q.Predicate),
		parseTerm(q.Object),
	))
	w.ds.Graphs[graph] = g
	return nil
}

func (w *Writer) Close() error {
	opts := gojsonld.NewOptions("")
	var data interface{}
	data = gojsonld.FromRDF(w.ds, opts)
	if w.ctx != nil {
		out, err := gojsonld.Compact(data, w.ctx, opts)
		if err != nil {
			return err
		}
		data = out
	}
	return json.NewEncoder(w.w).Encode(data)
}

func parseTerm(s string) gojsonld.Term {
	t := turtle.ParseTerm(s)
	switch v := t.(type) {
	case turtle.IRI:
		return gojsonld.NewResource(string(v))
	case turtle.BlankNode:
		return gojsonld.NewBlankNode(string(v))
	case turtle.Literal:
		if v.Language != "" {
			return gojsonld.NewLiteralWithLanguageAndDatatype(
				v.Value,
				v.Language,
				gojsonld.NewResource(gojsonld.XSD_STRING),
			)
		} else if v.DataType != "" {
			return gojsonld.NewLiteralWithDatatype(
				v.Value,
				gojsonld.NewResource(string(v.DataType)),
			)
		} else {
			return gojsonld.NewLiteralWithDatatype(
				v.Value,
				gojsonld.NewResource(gojsonld.XSD_STRING),
			)
		}
	default:
		return gojsonld.NewLiteralWithDatatype(s, gojsonld.NewResource(gojsonld.XSD_STRING))
	}
}
