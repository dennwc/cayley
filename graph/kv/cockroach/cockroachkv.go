// Copyright 2017 The Cayley Authors. All rights reserved.
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

package cockroachkv

import (
	"context"
	"fmt"
	"time"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/kv"
	"github.com/cayleygraph/cayley/graph/kv/cockroach/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	xcontext "golang.org/x/net/context"
	"google.golang.org/grpc"
)

func init() {
	kv.Register(Type, kv.Registration{
		NewFunc:      Open,
		InitFunc:     Create,
		IsPersistent: true,
	})
}

const (
	Type = "cockroachkv"
)

type DB struct {
	db   *client.DB
	conn *grpc.ClientConn
}

func dial(addr string) (*DB, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute/2)
	cc, err := grpc.DialContext(ctx, addr, grpc.WithInsecure())
	cancel()
	if err != nil {
		return nil, err
	}
	ss := client.NewSender(cc)
	db := client.NewDB(ss, hlc.NewClock(hlc.UnixNano, time.Millisecond*100))
	return &DB{
		db: db, conn: cc,
	}, nil
}

func Create(addr string, m graph.Options) (kv.BucketKV, error) {
	db, err := dial(addr)
	if err != nil {
		return nil, err
	}
	return kv.FromFlat(db), nil
}

func Open(addr string, m graph.Options) (kv.BucketKV, error) {
	db, err := dial(addr)
	if err != nil {
		return nil, err
	}
	return kv.FromFlat(db), nil
}

func (db *DB) Type() string {
	return Type
}
func (db *DB) Close() error {
	return db.conn.Close()
}
func (db *DB) Tx(update bool) (kv.FlatTx, error) {
	return &Tx{db: db, ro: !update}, nil
}

type operation struct {
	Del      bool
	Key, Val []byte
}

type Tx struct {
	db  *DB
	ro  bool
	ops []operation
	err error
}

func (tx *Tx) Commit() error {
	if tx.err != nil {
		return tx.err
	} else if tx.ro {
		return nil
	}
	tx.err = tx.db.db.Txn(context.TODO(), func(ctx xcontext.Context, txn *client.Txn) error {
		for _, op := range tx.ops {
			if op.Del {
				txn.Del(ctx, op.Key)
			} else {
				txn.Put(ctx, op.Key, op.Val)
			}
		}
		return nil
	})
	return tx.err
}
func (tx *Tx) Rollback() error {
	tx.ops = nil
	return tx.err
}
func (tx *Tx) Get(keys [][]byte) ([][]byte, error) {
	var b client.Batch
	for _, k := range keys {
		b.Get(k)
	}
	if err := tx.db.db.Run(context.TODO(), &b); err != nil {
		return nil, err
	} else if len(b.Results) != len(keys) {
		return nil, fmt.Errorf("unexpected number of results: %d", len(b.Results))
	}
	vals := make([][]byte, len(keys))
	for i, r := range b.Results {
		if r.Err != nil {
			return nil, r.Err
		}
		if len(r.Rows) == 0 {
			continue
		}
		vals[i] = r.Rows[0].ValueBytes()
	}
	return vals, nil
}
func (tx *Tx) Put(k, v []byte) error {
	if tx.ro {
		return fmt.Errorf("put on ro tx")
	}
	tx.ops = append(tx.ops, operation{Key: k, Val: v})
	return nil
}
func (tx *Tx) Del(k []byte) error {
	if tx.ro {
		return fmt.Errorf("del on ro tx")
	}
	tx.ops = append(tx.ops, operation{Del: true, Key: k})
	return nil
}

func (tx *Tx) Scan(pref []byte) kv.KVIterator {
	return &Iterator{tx: tx, start: pref, end: roachpb.Key(pref).PrefixEnd()}
}

type Iterator struct {
	tx    *Tx
	start []byte
	end   []byte
	done  bool
	buf   []client.KeyValue
	err   error
}

const pageSize = 100

func (it *Iterator) Next(ctx context.Context) bool {
	if it.err != nil {
		return false
	}
	if len(it.buf) > 1 {
		it.buf = it.buf[1:]
		return true
	}
	if it.done {
		return false
	}
	it.buf, it.err = it.tx.db.db.Scan(ctx, it.start, it.end, pageSize)
	if len(it.buf) > 0 {
		it.start = it.buf[len(it.buf)-1].Key.Next()
	}
	it.done = len(it.buf) == 0
	return !it.done
}
func (it *Iterator) Key() []byte {
	if len(it.buf) == 0 {
		return nil
	}
	return []byte(it.buf[0].Key)
}
func (it *Iterator) Val() []byte {
	if len(it.buf) == 0 {
		return nil
	}
	return it.buf[0].ValueBytes()
}
func (it *Iterator) Err() error {
	return it.err
}
func (it *Iterator) Close() error {
	it.buf = nil
	it.done = true
	return it.Err()
}
