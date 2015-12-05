package jsonld

import (
	"encoding/json"
	"github.com/google/cayley/quad"
	"github.com/linkeddata/gojsonld"
	"io"
	"strings"
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

func NewReader(r io.Reader) *Reader {
	var o interface{}
	if err := json.NewDecoder(r).Decode(&o); err != nil {
		return &Reader{err: err}
	}
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
	if len(s) <= 2 {
		return nil
	}
	// TODO(dennwc): parse for real
	if s[0] == '<' && s[len(s)-1] == '>' {
		return gojsonld.NewResource(s[1 : len(s)-1])
	} else if s[0] == '"' && s[len(s)-1] == '"' {
		return gojsonld.NewLiteralWithDatatype(
			unescape(s[1:len(s)-1]),
			gojsonld.NewResource(gojsonld.XSD_STRING),
		)
	} else if s[0] == '"' && s[len(s)-1] == '>' && strings.Index(s, `"^^<`) > 0 {
		i := strings.Index(s, `"^^<`)
		return gojsonld.NewLiteralWithDatatype(
			unescape(s[1:i]),
			gojsonld.NewResource(s[i+4:len(s)-1]),
		)
	} else if s[0] == '"' && strings.Index(s, `"@`) > 0 {
		i := strings.Index(s, `"@`)
		return gojsonld.NewLiteralWithLanguageAndDatatype(
			unescape(s[1:i]),
			s[i+2:],
			gojsonld.NewResource(gojsonld.XSD_STRING),
		)
	} else if strings.Index(s, "_:") == 0 {
		return gojsonld.NewBlankNode(s[2:])
	}
	return gojsonld.NewLiteralWithDatatype(s, gojsonld.NewResource(gojsonld.XSD_STRING))
}

func unescape(s string) string {
	s = strings.Replace(s, "\\\\", "\\", -1)
	s = strings.Replace(s, "\\\"", "\"", -1)
	s = strings.Replace(s, "\\n", "\n", -1)
	s = strings.Replace(s, "\\r", "\r", -1)
	s = strings.Replace(s, "\\t", "\t", -1)
	return s
}
