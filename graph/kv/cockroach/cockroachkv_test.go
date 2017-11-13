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
	"testing"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/kv"
	"github.com/cayleygraph/cayley/graph/kv/kvtest"
	"github.com/cayleygraph/cayley/internal/dock"
)

func makeCockroach(t testing.TB) (kv.BucketKV, graph.Options, func()) {
	var conf dock.Config

	conf.Image = "cockroachdb/cockroach:v1.0.5"
	conf.Cmd = []string{"start", "--insecure"}

	addr, closer := dock.RunAndWait(t, conf, dock.WaitPort("26257"))

	db, err := dial(addr + `:26257`)
	if err != nil {
		closer()
		t.Fatal(err)
	}

	return kv.FromFlat(db), nil, func() {
		closer()
	}
}

func TestCockroachKVAll(t *testing.T) {
	kvtest.TestAll(t, makeCockroach, nil)
}
