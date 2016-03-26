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

package leveldb

import (
	"io/ioutil"
	"os"
	"reflect"
	"sort"
	"testing"

	"github.com/google/cayley/graph"
	"github.com/google/cayley/graph/iterator"
	"github.com/google/cayley/quad"
	"github.com/google/cayley/writer"
)

func makeQuadSet() []quad.Quad {
	quadSet := []quad.Quad{
		quad.Make("A", "follows", "B", ""),
		quad.Make("C", "follows", "B", ""),
		quad.Make("C", "follows", "D", ""),
		quad.Make("D", "follows", "B", ""),
		quad.Make("B", "follows", "F", ""),
		quad.Make("F", "follows", "G", ""),
		quad.Make("D", "follows", "G", ""),
		quad.Make("E", "follows", "F", ""),
		quad.Make("B", "status", "cool", "status_graph"),
		quad.Make("D", "status", "cool", "status_graph"),
		quad.Make("G", "status", "cool", "status_graph"),
	}
	return quadSet
}

func iteratedQuads(qs graph.QuadStore, it graph.Iterator) []quad.Quad {
	var res quad.ByQuadString
	for graph.Next(it) {
		res = append(res, qs.Quad(it.Result()))
	}
	sort.Sort(res)
	return res
}

func iteratedNames(qs graph.QuadStore, it graph.Iterator) []string {
	var res []string
	for graph.Next(it) {
		res = append(res, quad.StringOf(qs.NameOf(it.Result())))
	}
	sort.Strings(res)
	return res
}

func TestCreateDatabase(t *testing.T) {
	tmpDir, err := ioutil.TempDir(os.TempDir(), "cayley_test")
	if err != nil {
		t.Fatalf("Could not create working directory: %v", err)
	}
	t.Log(tmpDir)

	err = createNewLevelDB(tmpDir, nil)
	if err != nil {
		t.Fatal("Failed to create LevelDB database.")
	}

	qs, err := newQuadStore(tmpDir, nil)
	if qs == nil || err != nil {
		t.Error("Failed to create leveldb QuadStore.")
	}
	if s := qs.Size(); s != 0 {
		t.Errorf("Unexpected size, got:%d expected:0", s)
	}
	qs.Close()

	err = createNewLevelDB("/dev/null/some terrible path", nil)
	if err == nil {
		t.Errorf("Created LevelDB database for bad path.")
	}

	os.RemoveAll(tmpDir)
}

func TestLoadDatabase(t *testing.T) {
	tmpDir, err := ioutil.TempDir(os.TempDir(), "cayley_test")
	if err != nil {
		t.Fatalf("Could not create working directory: %v", err)
	}
	defer os.RemoveAll(tmpDir)
	t.Log(tmpDir)

	err = createNewLevelDB(tmpDir, nil)
	if err != nil {
		t.Fatal("Failed to create LevelDB database.")
	}

	qs, err := newQuadStore(tmpDir, nil)
	if qs == nil || err != nil {
		t.Error("Failed to create leveldb QuadStore.")
	}

	w, _ := writer.NewSingleReplication(qs, nil)
	err = w.AddQuad(quad.Make(
		"Something",
		"points_to",
		"Something Else",
		"context",
	))
	if err != nil {
		t.Fatal("Failed to add quad:", err)
	}
	for _, pq := range []string{"Something", "points_to", "Something Else", "context"} {
		if got := quad.StringOf(qs.NameOf(qs.ValueOf(quad.Raw(pq)))); got != pq {
			t.Errorf("Failed to roundtrip %q, got:%q expect:%q", pq, got, pq)
		}
	}
	if s := qs.Size(); s != 1 {
		t.Errorf("Unexpected quadstore size, got:%d expect:1", s)
	}
	qs.Close()

	err = createNewLevelDB(tmpDir, nil)
	if err != graph.ErrDatabaseExists {
		t.Fatal("Failed to create LevelDB database.")
	}
	qs, err = newQuadStore(tmpDir, nil)
	if qs == nil || err != nil {
		t.Error("Failed to create leveldb QuadStore.")
	}
	w, _ = writer.NewSingleReplication(qs, nil)

	ts2, didConvert := qs.(*QuadStore)
	if !didConvert {
		t.Errorf("Could not convert from generic to LevelDB QuadStore")
	}

	//Test horizon
	horizon := qs.Horizon()
	if horizon.Int() != 1 {
		t.Errorf("Unexpected horizon value, got:%d expect:1", horizon.Int())
	}

	w.AddQuadSet(makeQuadSet())
	if s := qs.Size(); s != 12 {
		t.Errorf("Unexpected quadstore size, got:%d expect:12", s)
	}
	if s := ts2.SizeOf(qs.ValueOf(quad.Raw("B"))); s != 5 {
		t.Errorf("Unexpected quadstore size, got:%d expect:5", s)
	}
	horizon = qs.Horizon()
	if horizon.Int() != 12 {
		t.Errorf("Unexpected horizon value, got:%d expect:12", horizon.Int())
	}

	w.RemoveQuad(quad.Make(
		"A",
		"follows",
		"B",
		"",
	))
	if s := qs.Size(); s != 11 {
		t.Errorf("Unexpected quadstore size after RemoveQuad, got:%d expect:11", s)
	}
	if s := ts2.SizeOf(qs.ValueOf(quad.Raw("B"))); s != 4 {
		t.Errorf("Unexpected quadstore size, got:%d expect:4", s)
	}

	qs.Close()
}

func TestIterator(t *testing.T) {
	tmpDir, err := ioutil.TempDir(os.TempDir(), "cayley_test")
	if err != nil {
		t.Fatalf("Could not create working directory: %v", err)
	}
	defer os.RemoveAll(tmpDir)
	t.Log(tmpDir)

	err = createNewLevelDB(tmpDir, nil)
	if err != nil {
		t.Fatal("Failed to create LevelDB database.")
	}

	qs, err := newQuadStore(tmpDir, nil)
	if qs == nil || err != nil {
		t.Error("Failed to create leveldb QuadStore.")
	}

	w, _ := writer.NewSingleReplication(qs, nil)
	w.AddQuadSet(makeQuadSet())
	var it graph.Iterator

	it = qs.NodesAllIterator()
	if it == nil {
		t.Fatal("Got nil iterator.")
	}

	size, exact := it.Size()
	if size <= 0 || size >= 20 {
		t.Errorf("Unexpected size, got:%d expect:(0, 20)", size)
	}
	if exact {
		t.Errorf("Got unexpected exact result.")
	}
	if typ := it.Type(); typ != graph.All {
		t.Errorf("Unexpected iterator type, got:%v expect:%v", typ, graph.All)
	}
	optIt, changed := it.Optimize()
	if changed || optIt != it {
		t.Errorf("Optimize unexpectedly changed iterator.")
	}

	expect := []string{
		"A",
		"B",
		"C",
		"D",
		"E",
		"F",
		"G",
		"follows",
		"status",
		"cool",
		"status_graph",
	}
	sort.Strings(expect)
	for i := 0; i < 2; i++ {
		got := iteratedNames(qs, it)
		sort.Strings(got)
		if !reflect.DeepEqual(got, expect) {
			t.Errorf("Unexpected iterated result on repeat %d, got:%v expect:%v", i, got, expect)
		}
		it.Reset()
	}

	for _, pq := range expect {
		if !it.Contains(qs.ValueOf(quad.Raw(pq))) {
			t.Errorf("Failed to find and check %q correctly", pq)
		}
	}
	// FIXME(kortschak) Why does this fail?
	/*
		for _, pq := range []string{"baller"} {
			if it.Contains(qs.ValueOf(pq)) {
				t.Errorf("Failed to check %q correctly", pq)
			}
		}
	*/
	it.Reset()

	it = qs.QuadsAllIterator()
	graph.Next(it)
	q := qs.Quad(it.Result())
	set := makeQuadSet()
	var ok bool
	for _, t := range set {
		if t.String() == q.String() {
			ok = true
			break
		}
	}
	if !ok {
		t.Errorf("Failed to find %q during iteration, got:%q", q, set)
	}

	qs.Close()
}

func TestSetIterator(t *testing.T) {

	tmpDir, _ := ioutil.TempDir(os.TempDir(), "cayley_test")
	t.Log(tmpDir)
	defer os.RemoveAll(tmpDir)
	err := createNewLevelDB(tmpDir, nil)
	if err != nil {
		t.Fatalf("Failed to create working directory")
	}

	qs, err := newQuadStore(tmpDir, nil)
	if qs == nil || err != nil {
		t.Error("Failed to create leveldb QuadStore.")
	}
	defer qs.Close()

	w, _ := writer.NewSingleReplication(qs, nil)
	w.AddQuadSet(makeQuadSet())

	expect := []quad.Quad{
		quad.Make("C", "follows", "B", ""),
		quad.Make("C", "follows", "D", ""),
	}
	sort.Sort(quad.ByQuadString(expect))

	// Subject iterator.
	it := qs.QuadIterator(quad.Subject, qs.ValueOf(quad.Raw("C")))

	if got := iteratedQuads(qs, it); !reflect.DeepEqual(got, expect) {
		t.Errorf("Failed to get expected results, got:%v expect:%v", got, expect)
	}
	it.Reset()

	and := iterator.NewAnd(qs)
	and.AddSubIterator(qs.QuadsAllIterator())
	and.AddSubIterator(it)

	if got := iteratedQuads(qs, and); !reflect.DeepEqual(got, expect) {
		t.Errorf("Failed to get confirm expected results, got:%v expect:%v", got, expect)
	}

	// Object iterator.
	it = qs.QuadIterator(quad.Object, qs.ValueOf(quad.Raw("F")))

	expect = []quad.Quad{
		quad.Make("B", "follows", "F", ""),
		quad.Make("E", "follows", "F", ""),
	}
	sort.Sort(quad.ByQuadString(expect))
	if got := iteratedQuads(qs, it); !reflect.DeepEqual(got, expect) {
		t.Errorf("Failed to get expected results, got:%v expect:%v", got, expect)
	}

	and = iterator.NewAnd(qs)
	and.AddSubIterator(qs.QuadIterator(quad.Subject, qs.ValueOf(quad.Raw("B"))))
	and.AddSubIterator(it)

	expect = []quad.Quad{
		quad.Make("B", "follows", "F", ""),
	}
	if got := iteratedQuads(qs, and); !reflect.DeepEqual(got, expect) {
		t.Errorf("Failed to get confirm expected results, got:%v expect:%v", got, expect)
	}

	// Predicate iterator.
	it = qs.QuadIterator(quad.Predicate, qs.ValueOf(quad.Raw("status")))

	expect = []quad.Quad{
		quad.Make("B", "status", "cool", "status_graph"),
		quad.Make("D", "status", "cool", "status_graph"),
		quad.Make("G", "status", "cool", "status_graph"),
	}
	sort.Sort(quad.ByQuadString(expect))
	if got := iteratedQuads(qs, it); !reflect.DeepEqual(got, expect) {
		t.Errorf("Failed to get expected results from predicate iterator, got:%v expect:%v", got, expect)
	}

	// Label iterator.
	it = qs.QuadIterator(quad.Label, qs.ValueOf(quad.Raw("status_graph")))

	expect = []quad.Quad{
		quad.Make("B", "status", "cool", "status_graph"),
		quad.Make("D", "status", "cool", "status_graph"),
		quad.Make("G", "status", "cool", "status_graph"),
	}
	sort.Sort(quad.ByQuadString(expect))
	if got := iteratedQuads(qs, it); !reflect.DeepEqual(got, expect) {
		t.Errorf("Failed to get expected results from predicate iterator, got:%v expect:%v", got, expect)
	}
	it.Reset()

	// Order is important
	and = iterator.NewAnd(qs)
	and.AddSubIterator(qs.QuadIterator(quad.Subject, qs.ValueOf(quad.Raw("B"))))
	and.AddSubIterator(it)

	expect = []quad.Quad{
		quad.Make("B", "status", "cool", "status_graph"),
	}
	if got := iteratedQuads(qs, and); !reflect.DeepEqual(got, expect) {
		t.Errorf("Failed to get confirm expected results, got:%v expect:%v", got, expect)
	}
	it.Reset()

	// Order is important
	and = iterator.NewAnd(qs)
	and.AddSubIterator(it)
	and.AddSubIterator(qs.QuadIterator(quad.Subject, qs.ValueOf(quad.Raw("B"))))

	expect = []quad.Quad{
		quad.Make("B", "status", "cool", "status_graph"),
	}
	if got := iteratedQuads(qs, and); !reflect.DeepEqual(got, expect) {
		t.Errorf("Failed to get confirm expected results, got:%v expect:%v", got, expect)
	}
}

func TestOptimize(t *testing.T) {
	tmpDir, _ := ioutil.TempDir(os.TempDir(), "cayley_test")
	t.Log(tmpDir)
	defer os.RemoveAll(tmpDir)
	err := createNewLevelDB(tmpDir, nil)
	if err != nil {
		t.Fatalf("Failed to create working directory")
	}
	qs, err := newQuadStore(tmpDir, nil)
	if qs == nil || err != nil {
		t.Error("Failed to create leveldb QuadStore.")
	}

	w, _ := writer.NewSingleReplication(qs, nil)
	w.AddQuadSet(makeQuadSet())

	// With an linksto-fixed pair
	fixed := qs.FixedIterator()
	fixed.Add(qs.ValueOf(quad.Raw("F")))
	fixed.Tagger().Add("internal")
	lto := iterator.NewLinksTo(qs, fixed, quad.Object)

	oldIt := lto.Clone()
	newIt, ok := lto.Optimize()
	if !ok {
		t.Errorf("Failed to optimize iterator")
	}
	if newIt.Type() != Type() {
		t.Errorf("Optimized iterator type does not match original, got:%v expect:%v", newIt.Type(), Type())
	}

	newQuads := iteratedQuads(qs, newIt)
	oldQuads := iteratedQuads(qs, oldIt)
	if !reflect.DeepEqual(newQuads, oldQuads) {
		t.Errorf("Optimized iteration does not match original")
	}

	graph.Next(oldIt)
	oldResults := make(map[string]graph.Value)
	oldIt.TagResults(oldResults)
	graph.Next(newIt)
	newResults := make(map[string]graph.Value)
	newIt.TagResults(newResults)
	if !reflect.DeepEqual(newResults, oldResults) {
		t.Errorf("Discordant tag results, new:%v old:%v", newResults, oldResults)
	}
}
