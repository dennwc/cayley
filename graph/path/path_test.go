// Copyright 2014 The Cayley Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package path

import (
	"io"
	"os"
	"reflect"
	"sort"
	"testing"

	"github.com/google/cayley/graph"
	"github.com/google/cayley/quad"
	"github.com/google/cayley/quad/cquads"

	_ "github.com/google/cayley/graph/memstore"
	_ "github.com/google/cayley/writer"
)

// This is a simple test graph.
//
//  +-------+                        +------+
//  | alice |-----                 ->| fred |<--
//  +-------+     \---->+-------+-/  +------+   \-+-------+
//                ----->| #bob# |       |         | emily |
//  +---------+--/  --->+-------+       |         +-------+
//  | charlie |    /                    v
//  +---------+   /                  +--------+
//    \---    +--------+             | #greg# |
//        \-->| #dani# |------------>+--------+
//            +--------+

func loadGraph(path string, t testing.TB) []quad.Quad {
	var r io.Reader
	var simpleGraph []quad.Quad
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("Failed to open %q: %v", path, err)
	}
	defer f.Close()
	r = f

	dec := cquads.NewDecoder(r)
	for q1, err := dec.Unmarshal(); err == nil; q1, err = dec.Unmarshal() {
		simpleGraph = append(simpleGraph, q1)
	}
	if err != nil {
		t.Fatalf("Failed to Unmarshal: %v", err)
	}
	return simpleGraph
}

func makeTestStore(t testing.TB) graph.QuadStore {
	simpleGraph := loadGraph("../../data/testdata.nq", t)
	qs, _ := graph.NewQuadStore("memstore", "", nil)
	w, _ := graph.NewQuadWriter("single", qs, nil)
	for _, t := range simpleGraph {
		w.AddQuad(t)
	}
	return qs
}

func runTopLevel(path *Path) []quad.Value {
	var out []quad.Value
	it := path.BuildIterator()
	it, _ = it.Optimize()
	for graph.Next(it) {
		v := path.qs.NameOf(it.Result())
		out = append(out, v)
	}
	return out
}

func runTag(path *Path, tag string) []quad.Value {
	var out []quad.Value
	it := path.BuildIterator()
	it, _ = it.Optimize()
	for graph.Next(it) {
		tags := make(map[string]graph.Value)
		it.TagResults(tags)
		out = append(out, path.qs.NameOf(tags[tag]))
		for it.NextPath() {
			tags := make(map[string]graph.Value)
			it.TagResults(tags)
			out = append(out, path.qs.NameOf(tags[tag]))
		}
	}
	return out
}

type test struct {
	message string
	path    *Path
	expect  []quad.Value
	tag     string
}

// Define morphisms without a QuadStore

const (
	vFollows   = quad.IRI("follows")
	vAre       = quad.IRI("are")
	vStatus    = quad.IRI("status")
	vPredicate = quad.IRI("predicates")

	vCool       = quad.String("cool_person")
	vSmart      = quad.String("smart_person")
	vSmartGraph = quad.IRI("smart_graph")

	vAlice   = quad.IRI("alice")
	vBob     = quad.IRI("bob")
	vCharlie = quad.IRI("charlie")
	vDani    = quad.IRI("dani")
	vFred    = quad.IRI("fred")
	vGreg    = quad.IRI("greg")
	vEmily   = quad.IRI("emily")
)

var (
	grandfollows = StartMorphism().Out(vFollows).Out(vFollows)
)

func testSet(qs graph.QuadStore) []test {
	return []test{
		{
			message: "use out",
			path:    StartPathV(qs, vAlice).Out(vFollows),
			expect:  []quad.Value{vBob},
		},
		{
			message: "use in",
			path:    StartPathV(qs, vBob).In(vFollows),
			expect:  []quad.Value{vAlice, vCharlie, vDani},
		},
		{
			message: "use path Out",
			path:    StartPathV(qs, vBob).Out(StartPathV(qs, vPredicate).Out(vAre)),
			expect:  []quad.Value{vFred, vCool},
		},
		{
			message: "use And",
			path: StartPathV(qs, vDani).Out(vFollows).And(
				StartPathV(qs, vCharlie).Out(vFollows)),
			expect: []quad.Value{vBob},
		},
		{
			message: "use Or",
			path: StartPathV(qs, vFred).Out(vFollows).Or(
				StartPathV(qs, vAlice).Out(vFollows)),
			expect: []quad.Value{vBob, vGreg},
		},
		{
			message: "implicit All",
			path:    StartPathV(qs),
			expect:  []quad.Value{vAlice, vBob, vCharlie, vDani, vEmily, vFred, vGreg, vFollows, vStatus, vCool, vPredicate, vAre, vSmartGraph, vSmart},
		},
		{
			message: "follow",
			path:    StartPathV(qs, vCharlie).Follow(StartMorphism().Out(vFollows).Out(vFollows)),
			expect:  []quad.Value{vBob, vFred, vGreg},
		},
		{
			message: "followR",
			path:    StartPathV(qs, vFred).FollowReverse(StartMorphism().Out(vFollows).Out(vFollows)),
			expect:  []quad.Value{vAlice, vCharlie, vDani},
		},
		{
			message: "is, tag, instead of FollowR",
			path:    StartPathV(qs).Tag("first").Follow(StartMorphism().Out(vFollows).Out(vFollows)).IsV(vFred),
			expect:  []quad.Value{vAlice, vCharlie, vDani},
			tag:     "first",
		},
		{
			message: "use Except to filter out a single vertex",
			path:    StartPathV(qs, vAlice, vBob).Except(StartPathV(qs, vAlice)),
			expect:  []quad.Value{vBob},
		},
		{
			message: "use chained Except",
			path:    StartPathV(qs, vAlice, vBob, vCharlie).Except(StartPathV(qs, vBob)).Except(StartPathV(qs, vAlice)),
			expect:  []quad.Value{vCharlie},
		},
		{
			message: "show a simple save",
			path:    StartPathV(qs).Save(vStatus, "somecool"),
			tag:     "somecool",
			expect:  []quad.Value{vCool, vCool, vCool, vSmart, vSmart},
		},
		{
			message: "show a simple saveR",
			path:    StartPathV(qs, vCool).SaveReverse(vStatus, "who"),
			tag:     "who",
			expect:  []quad.Value{vGreg, vDani, vBob},
		},
		{
			message: "show a simple Has",
			path:    StartPathV(qs).HasV(vStatus, vCool),
			expect:  []quad.Value{vGreg, vDani, vBob},
		},
		{
			message: "show a double Has",
			path:    StartPathV(qs).HasV(vStatus, vCool).HasV(vFollows, vFred),
			expect:  []quad.Value{vBob},
		},
		{
			message: "use .Tag()-.Is()-.Back()",
			path:    StartPathV(qs, vBob).In(vFollows).Tag("foo").Out(vStatus).IsV(vCool).Back("foo"),
			expect:  []quad.Value{vDani},
		},
		{
			message: "do multiple .Back()s",
			path:    StartPathV(qs, vEmily).Out(vFollows).Tag("f").Out(vFollows).Out(vStatus).IsV(vCool).Back("f").In(vFollows).In(vFollows).Tag("acd").Out(vStatus).IsV(vCool).Back("f"),
			tag:     "acd",
			expect:  []quad.Value{vDani},
		},
		{
			message: "InPredicates()",
			path:    StartPathV(qs, vBob).InPredicates(),
			expect:  []quad.Value{vFollows},
		},
		{
			message: "OutPredicates()",
			path:    StartPathV(qs, vBob).OutPredicates(),
			expect:  []quad.Value{vFollows, vStatus},
		},
		// Morphism tests
		{
			message: "show simple morphism",
			path:    StartPathV(qs, vCharlie).Follow(grandfollows),
			expect:  []quad.Value{vGreg, vFred, vBob},
		},
		{
			message: "show reverse morphism",
			path:    StartPathV(qs, vFred).FollowReverse(grandfollows),
			expect:  []quad.Value{vAlice, vCharlie, vDani},
		},
		// Context tests
		{
			message: "query without label limitation",
			path:    StartPathV(qs, vGreg).Out(vStatus),
			expect:  []quad.Value{vSmart, vCool},
		},
		{
			message: "query with label limitation",
			path:    StartPathV(qs, vGreg).LabelContext(vSmartGraph).Out(vStatus),
			expect:  []quad.Value{vSmart},
		},
		{
			message: "reverse context",
			path:    StartPathV(qs, vGreg).Tag("base").LabelContext(vSmartGraph).Out(vStatus).Tag("status").Back("base"),
			expect:  []quad.Value{vGreg},
		},
	}
}

func TestMorphisms(t *testing.T) {
	qs := makeTestStore(t)
	for _, test := range testSet(qs) {
		var got []quad.Value
		if test.tag == "" {
			got = runTopLevel(test.path)
		} else {
			got = runTag(test.path, test.tag)
		}
		sort.Sort(quad.ByValueString(got))
		sort.Sort(quad.ByValueString(test.expect))
		if !reflect.DeepEqual(got, test.expect) {
			t.Errorf("Failed to %s, got: %v expected: %v", test.message, got, test.expect)
		}
	}
}
