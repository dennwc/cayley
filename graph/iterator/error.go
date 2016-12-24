package iterator

import (
	"fmt"
	"github.com/cayleygraph/cayley/graph"
)

func NewError(err error) *Error {
	return &Error{Reason: err}
}

type Error struct {
	Reason error
}

func (e Error) Err() error                      { return e.Reason }
func (e Error) Clone() graph.Iterator           { return e }
func (Error) Tagger() *graph.Tagger             { return nil }
func (Error) TagResults(map[string]graph.Value) {}
func (Error) Result() graph.Value               { return nil }
func (Error) Next() bool                        { return false }
func (Error) NextPath() bool                    { return false }
func (Error) Contains(graph.Value) bool         { return false }
func (Error) Reset()                            {}
func (Error) Stats() graph.IteratorStats {
	return graph.IteratorStats{}
}
func (Error) Size() (int64, bool)                { return 0, true }
func (Error) Type() graph.Type                   { return graph.Null }
func (e Error) Optimize() (graph.Iterator, bool) { return e, false }
func (Error) SubIterators() []graph.Iterator     { return nil }
func (e Error) Describe() graph.Description {
	return graph.Description{
		Name: fmt.Sprintf("error: %v", e.Reason),
	}
}
func (Error) Close() error { return nil }
func (Error) UID() uint64  { return 0 }
