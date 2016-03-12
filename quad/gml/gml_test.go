package gml_test

import (
	"bytes"
	"testing"

	"github.com/google/cayley/quad"
	"github.com/google/cayley/quad/gml"
)

var testData = []struct {
	quads []quad.Quad
	data  string
}{
	{
		[]quad.Quad{
			quad.Make(
				"_:subject1",
				"</film/performance/character>",
				`"Tomas de Torquemada"`,
				"",
			),
			quad.Make(
				"_:subject1",
				"<http://an.example/predicate1>",
				`"object1"`,
				"",
			),
			quad.Make(
				"<http://example.org/bob#me>",
				"<http://schema.org/birthDate>",
				`"1990-07-04"^^<http://www.w3.org/2001/XMLSchema#date>`,
				"",
			),
		},
		`Creator "Cayley"
graph [ directed 1
	node [ id 0 label "_:subject1" ]
	node [ id 1 label "&quot;Tomas de Torquemada&quot;" ]
	edge [ source 0 target 1 label "</film/performance/character>" ]
	node [ id 2 label "&quot;object1&quot;" ]
	edge [ source 0 target 2 label "<http://an.example/predicate1>" ]
	node [ id 3 label "<http://example.org/bob#me>" ]
	node [ id 4 label "&quot;1990-07-04&quot;^^<http://www.w3.org/2001/XMLSchema#date>" ]
	edge [ source 3 target 4 label "<http://schema.org/birthDate>" ]
]
`,
	},
}

func TestWriter(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	for _, c := range testData {
		buf.Reset()
		w := gml.NewWriter(buf)
		n, err := quad.Copy(w, quad.NewReader(c.quads))
		if err != nil {
			t.Fatalf("write failed after %d quads: %v", n, err)
		}
		if err = w.Close(); err != nil {
			t.Fatal("error on close:", err)
		}
		if c.data != buf.String() {
			t.Fatalf("wrong output:\n%s\n\nvs\n\n%s", buf.String(), c.data)
		}
	}
}
