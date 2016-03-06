package graphtest

import (
	"sort"
	"testing"

	"github.com/google/cayley/graph"
	"github.com/google/cayley/graph/iterator"
	"github.com/google/cayley/quad"
	"github.com/google/cayley/writer"
	"github.com/stretchr/testify/require"
)

type DatabaseFunc func(t testing.TB) (graph.QuadStore, func())

type Config struct {
	SkipDeletedFromIterator  bool
	SkipSizeCheckAfterDelete bool
}

func TestAll(t testing.TB, gen DatabaseFunc, conf *Config) {
	TestLoadOneQuad(t, gen)
	TestHorizonInt(t, gen, conf)
	TestIterator(t, gen)
	TestSetIterator(t, gen)
	if conf == nil || !conf.SkipDeletedFromIterator {
		TestDeletedFromIterator(t, gen)
	}
}

func MakeWriter(t testing.TB, qs graph.QuadStore, data ...quad.Quad) graph.QuadWriter {
	w, err := writer.NewSingleReplication(qs, nil)
	require.Nil(t, err)
	if len(data) > 0 {
		err = w.AddQuadSet(data)
		require.Nil(t, err)
	}
	return w
}

func MakeQuadSet() []quad.Quad {
	return []quad.Quad{
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
}

func IteratedQuads(t testing.TB, qs graph.QuadStore, it graph.Iterator) []quad.Quad {
	var res quad.ByQuadString
	for graph.Next(it) {
		res = append(res, qs.Quad(it.Result()))
	}
	require.Nil(t, it.Err())
	sort.Sort(res)
	return res
}

func ExpectIteratedQuads(t testing.TB, qs graph.QuadStore, it graph.Iterator, exp []quad.Quad) {
	sort.Sort(quad.ByQuadString(exp))
	got := IteratedQuads(t, qs, it)
	require.Equal(t, exp, got)
}

func IteratedRawStrings(t testing.TB, qs graph.QuadStore, it graph.Iterator) []string {
	var res []string
	for graph.Next(it) {
		res = append(res, qs.NameOf(it.Result()).String())
	}
	require.Nil(t, it.Err())
	sort.Strings(res)
	return res
}

func TestLoadOneQuad(t testing.TB, gen DatabaseFunc) {
	qs, closer := gen(t)
	defer closer()

	w := MakeWriter(t, qs)

	err := w.AddQuad(quad.Make(
		"Something",
		"points_to",
		"Something Else",
		"context",
	))
	require.Nil(t, err)
	for _, pq := range []string{"Something", "points_to", "Something Else", "context"} {
		got := qs.NameOf(qs.ValueOf(quad.Raw(pq))).String()
		require.Equal(t, pq, got, "Failed to roundtrip %q", pq)
	}
	require.Equal(t, int64(1), qs.Size(), "Unexpected quadstore size")
}

type ValueSizer interface {
	SizeOf(graph.Value) int64
}

func TestHorizonInt(t testing.TB, gen DatabaseFunc, conf *Config) {
	qs, closer := gen(t)
	defer closer()

	w := MakeWriter(t, qs)

	horizon := qs.Horizon()
	require.Equal(t, int64(0), horizon.Int(), "Unexpected horizon value")

	err := w.AddQuadSet(MakeQuadSet())
	require.Nil(t, err)
	require.Equal(t, int64(11), qs.Size(), "Unexpected quadstore size")

	if qss, ok := qs.(ValueSizer); ok {
		s := qss.SizeOf(qs.ValueOf(quad.Raw("B")))
		require.Equal(t, int64(5), s, "Unexpected quadstore value size")
	}

	horizon = qs.Horizon()
	require.Equal(t, int64(11), horizon.Int(), "Unexpected horizon value")

	err = w.RemoveQuad(quad.Make(
		"A",
		"follows",
		"B",
		"",
	))
	require.Nil(t, err)
	if conf == nil || !conf.SkipSizeCheckAfterDelete {
		require.Equal(t, int64(10), qs.Size(), "Unexpected quadstore size after RemoveQuad")
	} else {
		require.Equal(t, int64(11), qs.Size(), "Unexpected quadstore size")
	}

	if qss, ok := qs.(ValueSizer); ok {
		s := qss.SizeOf(qs.ValueOf(quad.Raw("B")))
		require.Equal(t, int64(4), s, "Unexpected quadstore value size")
	}
}

func TestIterator(t testing.TB, gen DatabaseFunc) {
	qs, closer := gen(t)
	defer closer()

	MakeWriter(t, qs, MakeQuadSet()...)

	var it graph.Iterator

	it = qs.NodesAllIterator()
	require.NotNil(t, it)

	size, _ := it.Size()
	require.True(t, size > 0 && size < 20, "Unexpected size")
	// TODO: leveldb had this test
	//if exact {
	//	t.Errorf("Got unexpected exact result.")
	//}
	require.Equal(t, graph.All, it.Type(), "Unexpected iterator type")

	optIt, changed := it.Optimize()
	require.True(t, !changed && optIt == it, "Optimize unexpectedly changed iterator: %v, %T", changed, optIt)

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
		got := IteratedRawStrings(t, qs, it)
		sort.Strings(got)
		require.Equal(t, expect, got, "Unexpected iterated result on repeat %d", i)
		it.Reset()
	}

	for _, pq := range expect {
		require.True(t, it.Contains(qs.ValueOf(quad.Raw(pq))), "Failed to find and check %q correctly", pq)

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
	optIt, changed = it.Optimize()
	require.True(t, !changed && optIt == it, "Optimize unexpectedly changed iterator: %v, %T", changed, optIt)

	graph.Next(it)
	q := qs.Quad(it.Result())
	set := MakeQuadSet()
	var ok bool
	for _, e := range set {
		if e.String() == q.String() {
			ok = true
			break
		}
	}
	require.True(t, ok, "Failed to find %q during iteration, got:%q", q, set)
}

func TestSetIterator(t testing.TB, gen DatabaseFunc) {
	qs, closer := gen(t)
	defer closer()

	MakeWriter(t, qs, MakeQuadSet()...)

	expectIteratedQuads := func(it graph.Iterator, exp []quad.Quad) {
		ExpectIteratedQuads(t, qs, it, exp)
	}

	// Subject iterator.
	it := qs.QuadIterator(quad.Subject, qs.ValueOf(quad.Raw("C")))

	expectIteratedQuads(it, []quad.Quad{
		quad.Make("C", "follows", "B", ""),
		quad.Make("C", "follows", "D", ""),
	})
	it.Reset()

	and := iterator.NewAnd(qs)
	and.AddSubIterator(qs.QuadsAllIterator())
	and.AddSubIterator(it)

	expectIteratedQuads(and, []quad.Quad{
		quad.Make("C", "follows", "B", ""),
		quad.Make("C", "follows", "D", ""),
	})

	// Object iterator.
	it = qs.QuadIterator(quad.Object, qs.ValueOf(quad.Raw("F")))

	expectIteratedQuads(it, []quad.Quad{
		quad.Make("B", "follows", "F", ""),
		quad.Make("E", "follows", "F", ""),
	})

	and = iterator.NewAnd(qs)
	and.AddSubIterator(qs.QuadIterator(quad.Subject, qs.ValueOf(quad.Raw("B"))))
	and.AddSubIterator(it)

	expectIteratedQuads(and, []quad.Quad{
		quad.Make("B", "follows", "F", ""),
	})

	// Predicate iterator.
	it = qs.QuadIterator(quad.Predicate, qs.ValueOf(quad.Raw("status")))

	expectIteratedQuads(it, []quad.Quad{
		quad.Make("B", "status", "cool", "status_graph"),
		quad.Make("D", "status", "cool", "status_graph"),
		quad.Make("G", "status", "cool", "status_graph"),
	})

	// Label iterator.
	it = qs.QuadIterator(quad.Label, qs.ValueOf(quad.Raw("status_graph")))

	expectIteratedQuads(it, []quad.Quad{
		quad.Make("B", "status", "cool", "status_graph"),
		quad.Make("D", "status", "cool", "status_graph"),
		quad.Make("G", "status", "cool", "status_graph"),
	})
	it.Reset()

	// Order is important
	and = iterator.NewAnd(qs)
	and.AddSubIterator(qs.QuadIterator(quad.Subject, qs.ValueOf(quad.Raw("B"))))
	and.AddSubIterator(it)

	expectIteratedQuads(and, []quad.Quad{
		quad.Make("B", "status", "cool", "status_graph"),
	})
	it.Reset()

	// Order is important
	and = iterator.NewAnd(qs)
	and.AddSubIterator(it)
	and.AddSubIterator(qs.QuadIterator(quad.Subject, qs.ValueOf(quad.Raw("B"))))

	expectIteratedQuads(and, []quad.Quad{
		quad.Make("B", "status", "cool", "status_graph"),
	})
}

func TestDeletedFromIterator(t testing.TB, gen DatabaseFunc) {
	qs, closer := gen(t)
	defer closer()

	w := MakeWriter(t, qs, MakeQuadSet()...)

	// Subject iterator.
	it := qs.QuadIterator(quad.Subject, qs.ValueOf(quad.Raw("E")))

	ExpectIteratedQuads(t, qs, it, []quad.Quad{
		quad.Make("E", "follows", "F", ""),
	})

	it.Reset()

	w.RemoveQuad(quad.Make("E", "follows", "F", ""))

	ExpectIteratedQuads(t, qs, it, nil)
}
