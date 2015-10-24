package graphml_test

import (
	"bytes"
	"testing"

	"github.com/google/cayley/quad"
	"github.com/google/cayley/quad/graphml"
)

var testData = []struct {
	quads []quad.Quad
	data  string
}{
	{
		[]quad.Quad{
			quad.Quad{
				Subject:   "_:subject1",
				Predicate: "</film/performance/character>",
				Object:    `"Tomás de Torquemada"`,
				Label:     "",
			},
			quad.Quad{
				Subject:   "_:subject1",
				Predicate: "<http://an.example/predicate1>",
				Object:    `"object1"`,
				Label:     "",
			},
			quad.Quad{
				Subject:   "<http://example.org/bob#me>",
				Predicate: "<http://schema.org/birthDate>",
				Object:    `"1990-07-04"^^<http://www.w3.org/2001/XMLSchema#date>`,
				Label:     "",
			},
		},
		`<?xml version="1.0" encoding="UTF-8"?>
<graphml xmlns="http://graphml.graphdrawing.org/xmlns"
	xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
	xsi:schemaLocation="http://graphml.graphdrawing.org/xmlns/1.0/graphml.xsd">
	<key id="d0" for="node" attr.name="description" attr.type="string"/>
	<key id="d1" for="edge" attr.name="description" attr.type="string"/>
	<graph id="G" edgedefault="directed">
		<node id="n0"><data key="d0">_:subject1</data></node>
		<node id="n1"><data key="d0">&#34;Tomás de Torquemada&#34;</data></node>
		<edge source="n0" target="n1"><data key="d1">&lt;/film/performance/character&gt;</data></edge>
		<node id="n2"><data key="d0">&#34;object1&#34;</data></node>
		<edge source="n0" target="n2"><data key="d1">&lt;http://an.example/predicate1&gt;</data></edge>
		<node id="n3"><data key="d0">&lt;http://example.org/bob#me&gt;</data></node>
		<node id="n4"><data key="d0">&#34;1990-07-04&#34;^^&lt;http://www.w3.org/2001/XMLSchema#date&gt;</data></node>
		<edge source="n3" target="n4"><data key="d1">&lt;http://schema.org/birthDate&gt;</data></edge>
	</graph>
</graphml>
`,
	},
}

func TestWriter(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	for _, c := range testData {
		buf.Reset()
		w := graphml.NewWriter(buf)
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
