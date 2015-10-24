package jsonarr

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/google/cayley/quad"
)

func init() {
	quad.RegisterFormat(quad.Format{
		Name:   "json",
		Ext:    []string{".json"},
		Mime:   []string{"application/json"},
		Writer: func(w io.Writer) quad.WriteCloser { return NewWriter(w) },
	})
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{w: w}
}

type Writer struct {
	w       io.Writer
	written bool
	closed  bool
}

func (w *Writer) WriteQuad(q quad.Quad) error {
	if w.closed {
		return fmt.Errorf("closed")
	}
	if !w.written {
		if _, err := w.w.Write([]byte("[")); err != nil {
			return err
		}
		w.written = true
	} else {
		if _, err := w.w.Write([]byte(",")); err != nil {
			return err
		}
	}
	data, err := json.Marshal(q)
	if err != nil {
		return err
	}
	_, err = w.w.Write(data)
	return err
}
func (w *Writer) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true
	if !w.written {
		_, err := w.w.Write([]byte("null\n"))
		return err
	}
	_, err := w.w.Write([]byte("]\n"))
	return err
}
