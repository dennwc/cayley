// Copyright 2015 The Cayley Authors. All rights reserved.
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

package bolt

import (
	"io/ioutil"
	"os"
	"reflect"
	"testing"

	"github.com/google/cayley/graph"
	"github.com/google/cayley/graph/graphtest"
	"github.com/google/cayley/graph/iterator"
	"github.com/google/cayley/quad"
	"github.com/google/cayley/writer"
)

var _ graphtest.ValueSizer = (*QuadStore)(nil)

func TestCreateDatabase(t *testing.T) {
	tmpFile, err := ioutil.TempFile(os.TempDir(), "cayley_test")
	if err != nil {
		t.Fatalf("Could not create working directory: %v", err)
	}
	t.Log(tmpFile)

	err = createNewBolt(tmpFile.Name(), nil)
	if err != nil {
		t.Fatal("Failed to create LevelDB database.")
	}

	qs, err := newQuadStore(tmpFile.Name(), nil)
	if qs == nil || err != nil {
		t.Error("Failed to create bolt QuadStore.")
	}
	if s := qs.Size(); s != 0 {
		t.Errorf("Unexpected size, got:%d expected:0", s)
	}
	qs.Close()

	err = createNewBolt("/dev/null/some terrible path", nil)
	if err == nil {
		t.Errorf("Created LevelDB database for bad path.")
	}

	os.RemoveAll(tmpFile.Name())
}

func makeBolt(t testing.TB) (graph.QuadStore, graph.Options, func()) {
	tmpFile, err := ioutil.TempFile(os.TempDir(), "cayley_test")
	if err != nil {
		t.Fatalf("Could not create working directory: %v", err)
	}
	err = createNewBolt(tmpFile.Name(), nil)
	if err != nil {
		os.RemoveAll(tmpFile.Name())
		t.Fatal("Failed to create Bolt database.", err)
	}
	qs, err := newQuadStore(tmpFile.Name(), nil)
	if qs == nil || err != nil {
		os.RemoveAll(tmpFile.Name())
		t.Fatal("Failed to create Bolt QuadStore.")
	}
	return qs, nil, func() {
		qs.Close()
		os.RemoveAll(tmpFile.Name())
	}
}

func TestBoltAll(t *testing.T) {
	graphtest.TestAll(t, makeBolt, &graphtest.Config{
		SkipNodeDelAfterQuadDel: true,
	})
}

func TestLoadDatabase(t *testing.T) {
	tmpFile, err := ioutil.TempFile(os.TempDir(), "cayley_test")
	if err != nil {
		t.Fatalf("Could not create working directory: %v", err)
	}
	defer os.RemoveAll(tmpFile.Name())
	t.Log(tmpFile.Name())

	err = createNewBolt(tmpFile.Name(), nil)
	if err != nil {
		t.Fatal("Failed to create Bolt database.", err)
	}

	qs, err := newQuadStore(tmpFile.Name(), nil)
	if qs == nil || err != nil {
		t.Error("Failed to create Bolt QuadStore.")
	}

	w, _ := writer.NewSingleReplication(qs, nil)
	w.WriteQuad(quad.Make(
		"Something",
		"points_to",
		"Something Else",
		"context",
	))
	for _, pq := range []string{"Something", "points_to", "Something Else", "context"} {
		if got := qs.NameOf(qs.ValueOf(quad.Raw(pq))).String(); got != pq {
			t.Errorf("Failed to roundtrip %q, got:%q expect:%q", pq, got, pq)
		}
	}
	if s := qs.Size(); s != 1 {
		t.Errorf("Unexpected quadstore size, got:%d expect:1", s)
	}
	qs.Close()

	err = createNewBolt(tmpFile.Name(), nil)
	if err != graph.ErrDatabaseExists {
		t.Fatal("Failed to create Bolt database.", err)
	}
	qs, err = newQuadStore(tmpFile.Name(), nil)
	if qs == nil || err != nil {
		t.Error("Failed to create Bolt QuadStore.")
	}
	w, _ = writer.NewSingleReplication(qs, nil)

	ts2, didConvert := qs.(*QuadStore)
	if !didConvert {
		t.Errorf("Could not convert from generic to Bolt QuadStore")
	}

	//Test horizon
	horizon := qs.Horizon()
	if horizon.Int() != 1 {
		t.Errorf("Unexpected horizon value, got:%d expect:1", horizon.Int())
	}

	w.WriteQuads(graphtest.MakeQuadSet())
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

func TestOptimize(t *testing.T) {
	qs, opts, closer := makeBolt(t)
	defer closer()

	graphtest.MakeWriter(t, qs, opts, graphtest.MakeQuadSet()...)

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

	newQuads := graphtest.IteratedQuads(t, qs, newIt)
	oldQuads := graphtest.IteratedQuads(t, qs, oldIt)
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
