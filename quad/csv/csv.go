package csv

import (
	"encoding/csv"
	"fmt"
	"github.com/cayleygraph/cayley/quad"
	"io"
)

func init() {
	quad.RegisterFormat(quad.Format{
		Name: "csv",
		Ext:  []string{".csv"},
		Mime: []string{"text/csv"},
		Reader: func(r io.Reader) quad.ReadCloser {
			return NewReader(csv.NewReader(r))
		},
	})
}

var _ quad.ReadCloser = (*Reader)(nil)

type Options struct {
	IDGen       func() quad.Value
	ValueParser func(string) quad.Value
}

func NewReader(cr *csv.Reader) *Reader {
	r := &Reader{r: cr}
	r.ValueParser = quad.StringToValue

	var last int64
	r.IDGen = func(_ []string) quad.Value {
		last++
		v := last
		return quad.BNode(fmt.Sprintf("n%d", v))
	}
	r.PredParser = func(s string) quad.Value {
		return quad.IRI(s)
	}
	return r
}

type Reader struct {
	r *csv.Reader

	IDGen       func(row []string) quad.Value
	ValueParser func(string) quad.Value
	PredParser  func(string) quad.Value
	RowMeta     func(sub quad.Value) []quad.Quad

	buf []quad.Quad

	n    int64
	sub  quad.Value
	pred []quad.Value
	rec  []string
	col  int
}

func (r *Reader) ReadQuad() (quad.Quad, error) {
	if r.pred == nil {
		// first row names predicates
		pred, err := r.r.Read()
		if err != nil {
			return quad.Quad{}, err
		}
		vpred := make([]quad.Value, 0, len(pred))
		for _, s := range pred {
			p := r.PredParser(s)
			if err != nil {
				return quad.Quad{}, err
			}
			vpred = append(vpred, p)
		}
		r.pred = vpred
	}
	if len(r.buf) > 0 {
		q := r.buf[0]
		r.buf = r.buf[1:]
		return q, nil
	}
	for {
		if r.col+1 >= len(r.rec) {
			rec, err := r.r.Read()
			if err != nil {
				return quad.Quad{}, err
			}
			r.n++
			r.col, r.rec = 0, rec
			r.sub = r.IDGen(rec)
			if r.RowMeta != nil {
				r.buf = append(r.buf, r.RowMeta(r.sub)...)
			}
		} else {
			r.col++
		}
		pred := r.pred[r.col]
		if pred == nil {
			continue
		}
		if val := r.ValueParser(r.rec[r.col]); val != nil {
			return quad.Quad{
				Subject:   r.sub,
				Predicate: pred,
				Object:    val,
			}, nil
		}
	}
}
func (r *Reader) Predicates() []quad.Value {
	return r.pred
}
func (r *Reader) Rows() int64 {
	return r.n
}
func (r *Reader) Close() error {
	return nil
}
