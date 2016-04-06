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
	"github.com/google/cayley/graph"
	"github.com/google/cayley/graph/iterator"
)

type AllIterator struct {
	iterator.Int64
	qs *QuadStore
}

type (
	nodesAllIterator AllIterator
	quadsAllIterator AllIterator
)

func newNodesAllIterator(qs *QuadStore) *nodesAllIterator {
	var out nodesAllIterator
	qs.idmu.RLock()
	id := qs.nextID - 1
	qs.idmu.RUnlock()
	out.Int64 = *iterator.NewInt64(1, id, true)
	out.qs = qs
	return &out
}

// No subiterators.
func (it *nodesAllIterator) SubIterators() []graph.Iterator {
	return nil
}

func (it *nodesAllIterator) Next() bool {
	if !it.Int64.Next() {
		return false
	}
	it.qs.idmu.RLock()
	_, ok := it.qs.revIDMap[int64(it.Int64.Result().(iterator.Int64Node))]
	it.qs.idmu.RUnlock()
	if !ok {
		return it.Next()
	}
	return true
}

func (it *nodesAllIterator) Err() error {
	return nil
}

func newQuadsAllIterator(qs *QuadStore) *quadsAllIterator {
	var out quadsAllIterator
	qs.logmu.RLock()
	id := qs.nextQuadID - 1
	qs.logmu.RUnlock()
	out.Int64 = *iterator.NewInt64(1, id, false)
	out.qs = qs
	return &out
}

func (it *quadsAllIterator) Next() (next bool) {
	for {
		next = it.Int64.Next()
		if !next {
			break
		}
		i64 := int64(it.Int64.Result().(iterator.Int64Quad))
		var skip bool
		it.qs.logmu.RLock()
		if i64 < int64(len(it.qs.log)) {
			skip = it.qs.log[i64].DeletedBy != 0 || it.qs.log[i64].Action == graph.Delete
		} else {
			next = false
		}
		it.qs.logmu.RUnlock()
		if !next || !skip {
			break
		}
	}
	return
}

// Override Optimize from it.Int64 - it will hide our Next implementation in other cases.

func (it *nodesAllIterator) Optimize() (graph.Iterator, bool) { return it, false }
func (it *quadsAllIterator) Optimize() (graph.Iterator, bool) { return it, false }

var _ graph.Nexter = &nodesAllIterator{}
var _ graph.Nexter = &quadsAllIterator{}
