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

package db

import (
	"errors"
	"fmt"

	"github.com/golang/glog"

	"github.com/google/cayley/graph"
	"github.com/google/cayley/internal/config"
	"github.com/google/cayley/quad"
)

var ErrNotPersistent = errors.New("database type is not persistent")

func Init(cfg *config.Config) error {
	if !graph.IsPersistent(cfg.DatabaseType) {
		return fmt.Errorf("ignoring unproductive database initialization request: %v", ErrNotPersistent)
	}

	return graph.InitQuadStore(cfg.DatabaseType, cfg.DatabasePath, cfg.DatabaseOptions)
}

func Open(cfg *config.Config) (*graph.Handle, error) {
	qs, err := OpenQuadStore(cfg)
	if err != nil {
		return nil, err
	}
	qw, err := OpenQuadWriter(qs, cfg)
	if err != nil {
		return nil, err
	}
	return &graph.Handle{QuadStore: qs, QuadWriter: qw}, nil
}

func OpenQuadStore(cfg *config.Config) (graph.QuadStore, error) {
	glog.Infof("Opening quad store %q at %s", cfg.DatabaseType, cfg.DatabasePath)
	qs, err := graph.NewQuadStore(cfg.DatabaseType, cfg.DatabasePath, cfg.DatabaseOptions)
	if err != nil {
		return nil, err
	}

	return qs, nil
}

func OpenQuadWriter(qs graph.QuadStore, cfg *config.Config) (graph.QuadWriter, error) {
	glog.Infof("Opening replication method %q", cfg.ReplicationType)
	w, err := graph.NewQuadWriter(cfg.ReplicationType, qs, cfg.ReplicationOptions)
	if err != nil {
		return nil, err
	}

	return w, nil
}

type batchLogger struct {
	cnt int
	quad.BatchWriter
}

func (w *batchLogger) WriteQuads(quads []quad.Quad) (int, error) {
	n, err := w.BatchWriter.WriteQuads(quads)
	if glog.V(2) {
		w.cnt += n
		glog.V(2).Infof("Wrote %d quads.", w.cnt)
	}
	if err != nil {
		err = fmt.Errorf("db: failed to load data: %v", err)
	}
	return n, err
}

func Load(qw graph.QuadWriter, cfg *config.Config, dec quad.Reader) error {
	_, err := quad.CopyBatch(&batchLogger{BatchWriter: qw}, dec, cfg.LoadSize)
	return err
}
