package pquads_test

import (
	"bytes"
	"github.com/google/cayley/quad"
	"github.com/google/cayley/quad/pquads"
	"io"
	"reflect"
	"testing"
)

var testData = []quad.Quad{
	{quad.BNode("one"), quad.IRI("name"), quad.String("One"), quad.IRI("graph")},
	{quad.BNode("one"), quad.IRI("rank"), quad.Int(101), nil},
	{quad.BNode("one"), quad.IRI("val"), quad.TypedString{Value: "123", Type: "int"}, nil},
}

func TestPQuads(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	w := pquads.NewWriter(buf)
	for _, q := range testData {
		if err := w.WriteQuad(q); err != nil {
			t.Fatal(err)
		}
	}
	var got []quad.Quad
	r := pquads.NewReader(buf)
	for {
		q, err := r.ReadQuad()
		if err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		got = append(got, q)
	}
	if !reflect.DeepEqual(got, testData) {
		t.Fatalf("unexpected data returned:\n%v\nvs\n%v", got, testData)
	}
}
