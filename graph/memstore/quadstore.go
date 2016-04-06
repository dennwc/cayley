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
	"errors"
	"sync"
	"time"

	"github.com/golang/glog"

	"github.com/google/cayley/graph"
	"github.com/google/cayley/graph/iterator"
	"github.com/google/cayley/graph/memstore/b"
	"github.com/google/cayley/quad"
)

const QuadStoreType = "memstore"

func init() {
	graph.RegisterQuadStore(QuadStoreType, graph.QuadStoreRegistration{
		NewFunc: func(string, graph.Options) (graph.QuadStore, error) {
			return newQuadStore(), nil
		},
		NewForRequestFunc: nil,
		UpgradeFunc:       nil,
		InitFunc:          nil,
		IsPersistent:      false,
	})
}

func cmp(a, b int64) int {
	return int(a - b)
}

type QuadDirectionIndex struct {
	mu    sync.RWMutex
	index [4]map[int64]*Tree
}

func NewQuadDirectionIndex() QuadDirectionIndex {
	return QuadDirectionIndex{index: [...]map[int64]*Tree{
		quad.Subject - 1:   make(map[int64]*Tree),
		quad.Predicate - 1: make(map[int64]*Tree),
		quad.Object - 1:    make(map[int64]*Tree),
		quad.Label - 1:     make(map[int64]*Tree),
	}}
}

type Tree struct {
	mu   sync.RWMutex
	tree *b.Tree
}

func (t *Tree) Len() int {
	t.mu.RLock()
	n := t.tree.Len()
	t.mu.RUnlock()
	return n
}
func (t *Tree) Set(id int64) {
	t.mu.Lock()
	t.tree.Set(id, struct{}{})
	t.mu.Unlock()
}
func (t *Tree) Contains(id int64) bool {
	t.mu.RLock()
	_, ok := t.tree.Get(id)
	t.mu.RUnlock()
	return ok
}
func (t *Tree) SeekFirst() *Enumerator {
	t.mu.RLock()
	iter, err := t.tree.SeekFirst()
	t.mu.RUnlock()
	if err != nil {
		return nil
	}
	return &Enumerator{tree: t, e: iter}
}
func (t *Tree) Seek(id int64) (*Enumerator, bool) {
	t.mu.RLock()
	iter, ok := t.tree.Seek(id)
	t.mu.RUnlock()
	if !ok {
		return nil, ok
	}
	return &Enumerator{tree: t, e: iter}, ok
}

type Enumerator struct {
	tree *Tree
	e    *b.Enumerator
}

func (e *Enumerator) Next() (int64, error) {
	e.tree.mu.RLock()
	result, _, err := e.e.Next()
	e.tree.mu.RUnlock()
	return result, err
}

func (qdi *QuadDirectionIndex) Tree(d quad.Direction, id int64) *Tree {
	if d < quad.Subject || d > quad.Label {
		panic("illegal direction")
	}
	qdi.mu.RLock()
	tree, ok := qdi.index[d-1][id]
	qdi.mu.RUnlock()
	if !ok {
		qdi.mu.Lock()
		tree, ok = qdi.index[d-1][id]
		if !ok {
			tree = &Tree{tree: b.TreeNew(cmp)}
			qdi.index[d-1][id] = tree
		}
		qdi.mu.Unlock()
	}
	return tree
}

func (qdi *QuadDirectionIndex) Get(d quad.Direction, id int64) (*Tree, bool) {
	if d < quad.Subject || d > quad.Label {
		panic("illegal direction")
	}
	qdi.mu.RLock()
	tree, ok := qdi.index[d-1][id]
	qdi.mu.RUnlock()
	return tree, ok
}

type LogEntry struct {
	ID        int64
	Quad      quad.Quad
	Action    graph.Procedure
	Timestamp time.Time
	DeletedBy int64
}

type QuadStore struct {
	idmu     sync.RWMutex
	idMap    map[string]int64
	revIDMap map[int64]quad.Value
	nextID   int64

	logmu      sync.RWMutex
	log        []LogEntry
	nextQuadID int64
	size       int64

	index QuadDirectionIndex
	// vip_index map[string]map[int64]map[string]map[int64]*b.Tree
}

func newQuadStore() *QuadStore {
	return &QuadStore{
		idMap:    make(map[string]int64),
		revIDMap: make(map[int64]quad.Value),

		// Sentinel null entry so indices start at 1
		log: make([]LogEntry, 1, 200),

		index:      NewQuadDirectionIndex(),
		nextID:     1,
		nextQuadID: 1,
	}
}

func (qs *QuadStore) ApplyDeltas(deltas []graph.Delta, ignoreOpts graph.IgnoreOpts) error {
	// Precheck the whole transaction
	for _, d := range deltas {
		switch d.Action {
		case graph.Add:
			if !ignoreOpts.IgnoreDup {
				if _, exists := qs.indexOf(d.Quad); exists {
					return graph.ErrQuadExists
				}
			}
		case graph.Delete:
			if !ignoreOpts.IgnoreMissing {
				if _, exists := qs.indexOf(d.Quad); !exists {
					return graph.ErrQuadNotExist
				}
			}
		default:
			return errors.New("memstore: invalid action")
		}
	}

	for _, d := range deltas {
		var err error
		switch d.Action {
		case graph.Add:
			err = qs.AddDelta(d)
			if err != nil && ignoreOpts.IgnoreDup {
				err = nil
			}
		case graph.Delete:
			err = qs.RemoveDelta(d)
			if err != nil && ignoreOpts.IgnoreMissing {
				err = nil
			}
		default:
			panic("memstore: unexpected invalid action")
		}
		if err != nil {
			return err
		}
	}
	return nil
}

const maxInt = int(^uint(0) >> 1)

func (qs *QuadStore) indexOf(t quad.Quad) (int64, bool) {
	min := maxInt
	var tree *Tree
	for d := quad.Subject; d <= quad.Label; d++ {
		sid := t.Get(d)
		if d == quad.Label && sid == nil {
			continue
		}
		qs.idmu.RLock()
		id, ok := qs.idMap[quad.StringOf(sid)]
		qs.idmu.RUnlock()
		// If we've never heard about a node, it must not exist
		if !ok {
			return 0, false
		}
		index, ok := qs.index.Get(d, id)
		if !ok {
			// If it's never been indexed in this direction, it can't exist.
			return 0, false
		}
		if l := index.Len(); l < min {
			min, tree = l, index
		}
	}

	it := NewIterator(tree, qs, 0, nil)
	for it.Next() {
		qs.logmu.RLock()
		l := qs.log[it.result]
		qs.logmu.RUnlock()
		if t == l.Quad {
			return it.result, true
		}
	}
	return 0, false
}

func (qs *QuadStore) AddDelta(d graph.Delta) error {
	if _, exists := qs.indexOf(d.Quad); exists {
		return graph.ErrQuadExists
	}
	qs.logmu.Lock()
	qid := qs.nextQuadID
	qs.log = append(qs.log, LogEntry{
		ID:        d.ID.Int(),
		Quad:      d.Quad,
		Action:    d.Action,
		Timestamp: d.Timestamp})
	qs.size++
	qs.nextQuadID++
	qs.logmu.Unlock()

	for dir := quad.Subject; dir <= quad.Label; dir++ {
		sid := d.Quad.Get(dir)
		if dir == quad.Label && sid == nil {
			continue
		}
		ssid := quad.StringOf(sid)
		qs.idmu.RLock()
		id, ok := qs.idMap[ssid]
		qs.idmu.RUnlock()
		if !ok {
			qs.idmu.Lock()
			id, ok = qs.idMap[ssid]
			if !ok {
				id = qs.nextID
				qs.idMap[ssid] = qs.nextID
				qs.revIDMap[qs.nextID] = sid
				qs.nextID++
			}
			qs.idmu.Unlock()
		}
		qs.index.Tree(dir, id).Set(qid)
	}

	// TODO(barakmich): Add VIP indexing
	return nil
}

func (qs *QuadStore) RemoveDelta(d graph.Delta) error {
	prevQuadID, exists := qs.indexOf(d.Quad)
	if !exists {
		return graph.ErrQuadNotExist
	}
	qs.logmu.Lock()
	quadID := qs.nextQuadID
	qs.log = append(qs.log, LogEntry{
		ID:        d.ID.Int(),
		Quad:      d.Quad,
		Action:    d.Action,
		Timestamp: d.Timestamp})
	qs.log[prevQuadID].DeletedBy = quadID
	qs.size--
	qs.nextQuadID++
	qs.logmu.Unlock()
	return nil
}

func (qs *QuadStore) Quad(index graph.Value) quad.Quad {
	qs.logmu.RLock()
	q := qs.log[index.(iterator.Int64Quad)].Quad
	qs.logmu.RUnlock()
	return q
}

func (qs *QuadStore) QuadIterator(d quad.Direction, value graph.Value) graph.Iterator {
	index, ok := qs.index.Get(d, int64(value.(iterator.Int64Node)))
	if ok {
		return NewIterator(index, qs, d, value)
	}
	return &iterator.Null{}
}

func (qs *QuadStore) Horizon() graph.PrimaryKey {
	qs.logmu.RLock()
	id := qs.log[len(qs.log)-1].ID
	qs.logmu.RUnlock()
	return graph.NewSequentialKey(id)
}

func (qs *QuadStore) Size() int64 {
	qs.logmu.RLock()
	size := qs.size
	qs.logmu.RUnlock()
	return size
}

func (qs *QuadStore) DebugPrint() {
	qs.logmu.RLock()
	defer qs.logmu.RUnlock()
	for i, l := range qs.log {
		if i == 0 {
			continue
		}
		glog.V(2).Infof("%d: %#v", i, l)
	}
}

func (qs *QuadStore) ValueOf(name quad.Value) graph.Value {
	qs.idmu.RLock()
	v := qs.idMap[quad.StringOf(name)]
	qs.idmu.RUnlock()
	return iterator.Int64Node(v)
}

func (qs *QuadStore) NameOf(id graph.Value) quad.Value {
	if id == nil {
		return nil
	}
	qs.idmu.RLock()
	v := qs.revIDMap[int64(id.(iterator.Int64Node))]
	qs.idmu.RUnlock()
	return v
}

func (qs *QuadStore) QuadsAllIterator() graph.Iterator {
	return newQuadsAllIterator(qs)
}

func (qs *QuadStore) FixedIterator() graph.FixedIterator {
	return iterator.NewFixed(iterator.Identity)
}

func (qs *QuadStore) QuadDirection(val graph.Value, d quad.Direction) graph.Value {
	name := qs.Quad(val).Get(d)
	return qs.ValueOf(name)
}

func (qs *QuadStore) NodesAllIterator() graph.Iterator {
	return newNodesAllIterator(qs)
}

func (qs *QuadStore) Close() {}

func (qs *QuadStore) Type() string {
	return QuadStoreType
}
