// Package jsonld provides an encoder/decoder for JSON-LD quad format
package jsonld

import (
	"encoding/json"
	"fmt"
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

func toQuadValue(t gojsonld.Term) quad.Value {
	if t == nil {
		return nil
	}
	switch v := t.(type) {
	case *gojsonld.Resource:
		return quad.IRI(v.URI)
	case *gojsonld.Literal:
		val := quad.String(v.Value)
		if v.Language != "" {
			return quad.LangString{Value: val, Lang: v.Language}
		} else if v.Datatype != nil {
			return quad.TypedString{Value: val, Type: toQuadValue(v.Datatype).(quad.IRI)}
		}
		return val
	case *gojsonld.BlankNode:
		return quad.BNode(v.ID)
	default:
		panic(fmt.Errorf("unknown term type: %T", t))
	}
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
	var graph quad.Value
	if r.name != "" && r.name != "@default" {
		graph = quad.Raw(r.name)
	}
	return quad.Quad{
		Subject:   toQuadValue(cur.Subject),
		Predicate: toQuadValue(cur.Predicate),
		Object:    toQuadValue(cur.Object),
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
	graph := quad.StringOf(q.Label)
	if graph == "" {
		graph = "@default"
	}
	g := w.ds.Graphs[graph]
	g = append(g, gojsonld.NewTriple(
		toTerm(q.Subject),
		toTerm(q.Predicate),
		toTerm(q.Object),
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

func toTerm(qv quad.Value) gojsonld.Term {
	if qv == nil {
		return nil
	}
	switch v := qv.(type) {
	case quad.IRI:
		return gojsonld.NewResource(string(v))
	case quad.BNode:
		return gojsonld.NewBlankNode(string(v))
	case quad.TypedString:
		return gojsonld.NewLiteralWithDatatype(
			string(v.Value),
			gojsonld.NewResource(string(v.Type)),
		)
	case quad.LangString:
		return gojsonld.NewLiteralWithLanguageAndDatatype(
			string(v.Value),
			string(v.Lang),
			gojsonld.NewResource(gojsonld.XSD_STRING),
		)
	case quad.String:
		return gojsonld.NewLiteralWithDatatype(
			string(v),
			gojsonld.NewResource(gojsonld.XSD_STRING),
		)
	}
	t, err := turtle.Parse(quad.StringOf(qv))
	if err != nil {
		return gojsonld.NewLiteralWithDatatype(
			string(quad.StringOf(qv)),
			gojsonld.NewResource(gojsonld.XSD_STRING),
		)
	}
	switch v := t.(type) {
	case turtle.IRI:
		return gojsonld.NewResource(string(v))
	case turtle.BlankNode:
		return gojsonld.NewBlankNode(string(v))
	case turtle.String:
		return gojsonld.NewLiteralWithDatatype(
			string(v),
			gojsonld.NewResource(gojsonld.XSD_STRING),
		)
	case turtle.LangString:
		return gojsonld.NewLiteralWithLanguageAndDatatype(
			string(v.Value),
			v.Lang,
			gojsonld.NewResource(gojsonld.XSD_STRING),
		)
	case turtle.TypedString:
		return gojsonld.NewLiteralWithDatatype(
			string(v.Value),
			gojsonld.NewResource(string(v.Type)),
		)
	default:
		return gojsonld.NewLiteralWithDatatype(quad.StringOf(qv), gojsonld.NewResource(gojsonld.XSD_STRING))
	}
}
