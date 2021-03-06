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

package memstore

import (
	"fmt"
	"io"
	"math"

	"github.com/google/cayley/graph"
	"github.com/google/cayley/graph/iterator"
	"github.com/google/cayley/quad"
)

type Iterator struct {
	nodes  bool
	uid    uint64
	qs     *QuadStore
	tags   graph.Tagger
	tree   *Tree
	iter   *Enumerator
	result int64
	err    error

	d     quad.Direction
	value graph.Value
}

func NewIterator(tree *Tree, qs *QuadStore, d quad.Direction, value graph.Value) *Iterator {
	iter := tree.SeekFirst()
	return &Iterator{
		nodes: d == 0,
		uid:   iterator.NextUID(),
		qs:    qs,
		tree:  tree,
		iter:  iter,
		d:     d,
		value: value,
	}
}

func (it *Iterator) UID() uint64 {
	return it.uid
}

func (it *Iterator) Reset() {
	it.iter = it.tree.SeekFirst()
}

func (it *Iterator) Tagger() *graph.Tagger {
	return &it.tags
}

func (it *Iterator) TagResults(dst map[string]graph.Value) {
	for _, tag := range it.tags.Tags() {
		dst[tag] = it.Result()
	}

	for tag, value := range it.tags.Fixed() {
		dst[tag] = value
	}
}

func (it *Iterator) Clone() graph.Iterator {
	var iter *Enumerator
	if it.result > 0 {
		var ok bool
		iter, ok = it.tree.Seek(it.result)
		if !ok {
			panic("value unexpectedly missing")
		}
	} else {
		iter = it.tree.SeekFirst()
	}

	m := &Iterator{
		uid:   iterator.NextUID(),
		qs:    it.qs,
		tree:  it.tree,
		iter:  iter,
		d:     it.d,
		value: it.value,
	}
	m.tags.CopyFrom(it)

	return m
}

func (it *Iterator) Close() error {
	return nil
}

func (it *Iterator) checkValid(index int64) bool {
	it.qs.logmu.RLock()
	valid := it.qs.log[index].DeletedBy == 0
	it.qs.logmu.RUnlock()
	return valid
}

func (it *Iterator) Next() bool {
	graph.NextLogIn(it)

	if it.iter == nil {
		return graph.NextLogOut(it, nil, false)
	}
	result, err := it.iter.Next()
	if err != nil {
		if err != io.EOF {
			it.err = err
		}
		return graph.NextLogOut(it, nil, false)
	}
	if !it.checkValid(result) {
		return it.Next()
	}
	it.result = result
	return graph.NextLogOut(it, it.Result(), true)
}

func (it *Iterator) Err() error {
	return it.err
}

func (it *Iterator) Result() graph.Value {
	if it.nodes {
		return iterator.Int64Node(it.result)
	}
	return iterator.Int64Quad(it.result)
}

func (it *Iterator) NextPath() bool {
	return false
}

// No subiterators.
func (it *Iterator) SubIterators() []graph.Iterator {
	return nil
}

func (it *Iterator) Size() (int64, bool) {
	return int64(it.tree.Len()), true
}

func (it *Iterator) Contains(v graph.Value) bool {
	graph.ContainsLogIn(it, v)
	if v == nil {
		return graph.ContainsLogOut(it, v, false)
	} else if it.nodes != v.IsNode() {
		return graph.ContainsLogOut(it, v, false)
	}
	var vi int64
	if it.nodes {
		vi = int64(v.(iterator.Int64Node))
	} else {
		vi = int64(v.(iterator.Int64Quad))
	}
	if it.tree.Contains(vi) {
		it.result = vi
		return graph.ContainsLogOut(it, v, true)
	}
	return graph.ContainsLogOut(it, v, false)
}

func (it *Iterator) Describe() graph.Description {
	size, _ := it.Size()
	return graph.Description{
		UID:  it.UID(),
		Name: fmt.Sprintf("dir:%s val:%d", it.d, it.value),
		Type: it.Type(),
		Tags: it.tags.Tags(),
		Size: size,
	}
}

var memType graph.Type

func init() {
	memType = graph.RegisterIterator("b+tree")
}

func Type() graph.Type { return memType }

func (it *Iterator) Type() graph.Type { return memType }

func (it *Iterator) Sorted() bool { return true }

func (it *Iterator) Optimize() (graph.Iterator, bool) {
	return it, false
}

func (it *Iterator) Stats() graph.IteratorStats {
	return graph.IteratorStats{
		ContainsCost: int64(math.Log(float64(it.tree.Len()))) + 1,
		NextCost:     1,
		Size:         int64(it.tree.Len()),
	}
}

var _ graph.Nexter = &Iterator{}
