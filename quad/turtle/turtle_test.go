package turtle

import "testing"

var testCases = []struct {
	text string
	term Term
}{
	{`_:100000`, BlankNode(`100000`)},
	{`_:subject1`, BlankNode(`subject1`)},
	// TODO(dennwc): handle IRIs with prefixes
	//	{`rdf:subject5`,IRI(`rdf:subject5`)},
	//	{`:subject5`,IRI(`:subject5`)},
	{`</film/performance/actor>`, IRI(`/film/performance/actor`)},
	{`<http://one.example/subject1>`, IRI(`http://one.example/subject1`)},
	{`<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>`, IRI(`http://www.w3.org/1999/02/22-rdf-syntax-ns#type`)},
	{`<https://www.wikidata.org/wiki/Special:EntityData/Q12418>`, IRI(`https://www.wikidata.org/wiki/Special:EntityData/Q12418`)},
	{`"Tomás de Torquemada"`, Literal{Value: `Tomás de Torquemada`}},
	{"\"object of some real\\tlength\"@en", Literal{Value: "object of some real\tlength", Language: "en"}},
	// TODO(dennwc): handle single quotes
	//	{`'Cette Série des Années Soixante-dix'@fr`,Literal{Value:`Cette Série des Années Soixante-dix`,Language:"fr"}},
	{`"Cette Série des Années Septante"@fr-be`, Literal{Value: `Cette Série des Années Septante`, Language: "fr-be"}},
	// TODO(dennwc): handle IRIs with prefixes
	//	{`"That Seventies Show"^^xsd:string`,Literal{Value:`That Seventies Show`,DataType:IRI(`xsd:string`)}},
	{`"1990-07-04"^^<http://www.w3.org/2001/XMLSchema#date>`, Literal{Value: `1990-07-04`, DataType: IRI(`http://www.w3.org/2001/XMLSchema#date`)}},
}

func TestParse(t *testing.T) {
	for i, c := range testCases {
		if term := ParseTerm(c.text); term != c.term {
			t.Errorf("case %d failed: %#v(%T) vs %#v(%T)", i, term, term, c.term, c.term)
		}
	}
}

func TestPrint(t *testing.T) {
	for i, c := range testCases {
		if s := c.term.String(); s != c.text {
			t.Errorf("case %d failed: %v vs %v", i, s, c.text)
		}
	}
}
