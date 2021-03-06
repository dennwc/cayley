package jsonld

import (
	"bytes"
	"encoding/json"
	"github.com/google/cayley/quad"
	"reflect"
	"sort"
	"strings"
	"testing"
)

var testReadCases = []struct {
	data   string
	expect []quad.Quad
}{
	{
		`{
  "@context": {
    "ex": "http://example.org/",
    "term1": {"@id": "ex:term1", "@type": "ex:datatype"},
    "term2": {"@id": "ex:term2", "@type": "@id"},
    "term3": {"@id": "ex:term3", "@language": "en"}
  },
  "@id": "ex:id1",
  "@type": ["ex:Type1", "ex:Type2"],
  "term1": "v1",
  "term2": "ex:id2",
  "term3": "v3"
}`,
		[]quad.Quad{
			{quad.IRI(`http://example.org/id1`), quad.IRI(`http://example.org/term1`), quad.TypedString{Value: "v1", Type: `http://example.org/datatype`}, nil},
			{quad.IRI(`http://example.org/id1`), quad.IRI(`http://example.org/term2`), quad.IRI(`http://example.org/id2`), nil},
			{quad.IRI(`http://example.org/id1`), quad.IRI(`http://example.org/term3`), quad.LangString{Value: "v3", Lang: "en"}, nil},
			{quad.IRI(`http://example.org/id1`), quad.IRI(`http://www.w3.org/1999/02/22-rdf-syntax-ns#type`), quad.IRI(`http://example.org/Type1`), nil},
			{quad.IRI(`http://example.org/id1`), quad.IRI(`http://www.w3.org/1999/02/22-rdf-syntax-ns#type`), quad.IRI(`http://example.org/Type2`), nil},
		},
	},
}

type ByQuad []quad.Quad

func (a ByQuad) Len() int           { return len(a) }
func (a ByQuad) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByQuad) Less(i, j int) bool { return a[i].NQuad() < a[j].NQuad() }

func TestRead(t *testing.T) {
	for i, c := range testReadCases {
		r := NewReader(strings.NewReader(c.data))
		quads, err := quad.ReadAll(r)
		if err != nil {
			t.Errorf("case %d failed: %v", i, err)
		}
		sort.Sort(ByQuad(quads))
		sort.Sort(ByQuad(c.expect))
		if !reflect.DeepEqual(quads, c.expect) {
			for _, q := range quads {
				t.Errorf("%+v (%T %T %T %T)", q, q.Subject, q.Predicate, q.Object, q.Label)
			}
			t.Errorf("case %d failed: wrong quads returned:\n%v\n%v", i, quads, c.expect)
		}
		r.Close()
	}
}

var testWriteCases = []struct {
	data   []quad.Quad
	ctx    interface{}
	expect string
}{
	{
		[]quad.Quad{
			quad.Make(`<http://example.org/id1>`, `<http://example.org/term1>`, `"v1"^^<http://example.org/datatype>`, ``),
			quad.Make(`<http://example.org/id1>`, `<http://example.org/term2>`, `<http://example.org/id2>`, ``),
			quad.Make(`<http://example.org/id1>`, `<http://example.org/term3>`, `"v3"@en`, ``),
			quad.Make(`<http://example.org/id1>`, `<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>`, `<http://example.org/Type1>`, ``),
			quad.Make(`<http://example.org/id1>`, `<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>`, `<http://example.org/Type2>`, ``),
		},
		map[string]interface{}{
			"ex": "http://example.org/",
		},
		`{
  "@context": {
    "ex": "http://example.org/"
  },
  "@id": "ex:id1",
  "@type": [
    "ex:Type1",
    "ex:Type2"
  ],
  "ex:term1": {
    "@type": "ex:datatype",
    "@value": "v1"
  },
  "ex:term2": {
    "@id": "ex:id2"
  },
  "ex:term3": {
    "@language": "en",
    "@value": "v3"
  }
}
`,
	},
}

func TestWrite(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	for i, c := range testWriteCases {
		buf.Reset()
		w := NewWriter(buf)
		w.SetLdContext(c.ctx)
		_, err := quad.Copy(w, quad.NewReader(c.data))
		if err != nil {
			t.Errorf("case %d failed: %v", i, err)
		} else if err = w.Close(); err != nil {
			t.Errorf("case %d failed: %v", i, err)
		}
		data := make([]byte, buf.Len())
		copy(data, buf.Bytes())
		buf.Reset()
		json.Indent(buf, data, "", "  ")
		if buf.String() != c.expect {
			t.Errorf("case %d failed: wrong data returned:\n%v\n%v", i, buf.String(), c.expect)
		}
	}
}
