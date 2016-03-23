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

//go:generate ragel -Z -G2 parse.rl

// Package cquads implements parsing N-Quads like line-based syntax
// for RDF datasets.
//
// N-Quad parsing is performed as based on a simplified grammar derived from
// the N-Quads grammar defined by http://www.w3.org/TR/n-quads/.
//
// For a complete definition of the grammar, see cquads.rl.
package cquads

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strconv"

	"github.com/google/cayley/quad"
)

func init() {
	quad.RegisterFormat(quad.Format{
		Name:   "cquads",
		Ext:    []string{".nq", ".nt"},
		Mime:   []string{"application/n-quads", "application/n-triples"},
		Reader: func(r io.Reader) quad.ReadCloser { return NewDecoder(r) },
		Writer: func(w io.Writer) quad.WriteCloser { return NewEncoder(w) },
	})
}

// AutoConvertTypedString allows to convert TypedString values to native
// equivalents directly while parsing. It will call ToNative on all TypedString values.
//
// If conversion error occurs, it will preserve original TypedString value.
var AutoConvertTypedString = true

// Decoder implements simplified N-Quad document parsing.
type Decoder struct {
	r    *bufio.Reader
	line []byte
}

// NewDecoder returns an N-Quad decoder that takes its input from the
// provided io.Reader.
func NewDecoder(r io.Reader) *Decoder {
	return &Decoder{r: bufio.NewReader(r)}
}

// ReadQuad returns the next valid N-Quad as a quad.Quad, or an error.
func (dec *Decoder) ReadQuad() (quad.Quad, error) {
	dec.line = dec.line[:0]
	var line []byte
	for {
		for {
			l, pre, err := dec.r.ReadLine()
			if err != nil {
				return quad.Quad{}, err
			}
			dec.line = append(dec.line, l...)
			if !pre {
				break
			}
		}
		if line = bytes.TrimSpace(dec.line); len(line) != 0 && line[0] != '#' {
			break
		}
		dec.line = dec.line[:0]
	}
	q, err := Parse(string(line))
	if err != nil {
		return quad.Quad{}, fmt.Errorf("failed to parse %q: %v", dec.line, err)
	}
	if !q.IsValid() {
		return dec.ReadQuad()
	}
	return q, nil
}
func (dec *Decoder) Close() error { return nil }

// Unmarshal returns the next valid N-Quad as a quad.Quad, or an error.
//
// Deprecated: use ReadQuad instead.
func (dec *Decoder) Unmarshal() (quad.Quad, error) {
	return dec.ReadQuad()
}

func unEscape(r []rune, spec int, isQuoted, isEscaped bool) quad.Value {
	raw := r
	var sp []rune
	if spec > 0 {
		r, sp = r[:spec], r[spec:]
		isQuoted = true
	}
	if isQuoted {
		r = r[1 : len(r)-1]
	} else {
		if len(r) >= 2 && r[0] == '<' && r[len(r)-1] == '>' {
			return quad.IRI(r[1 : len(r)-1])
		}
		if len(r) >= 2 && r[0] == '_' && r[1] == ':' {
			return quad.BNode(string(r[2:]))
		}
	}
	var val string
	if isEscaped {
		buf := bytes.NewBuffer(make([]byte, 0, len(r)))

		for i := 0; i < len(r); {
			switch r[i] {
			case '\\':
				i++
				var c byte
				switch r[i] {
				case 't':
					c = '\t'
				case 'b':
					c = '\b'
				case 'n':
					c = '\n'
				case 'r':
					c = '\r'
				case 'f':
					c = '\f'
				case '"':
					c = '"'
				case '\'':
					c = '\''
				case '\\':
					c = '\\'
				case 'u':
					rc, err := strconv.ParseInt(string(r[i+1:i+5]), 16, 32)
					if err != nil {
						panic(fmt.Errorf("internal parser error: %v", err))
					}
					buf.WriteRune(rune(rc))
					i += 5
					continue
				case 'U':
					rc, err := strconv.ParseInt(string(r[i+1:i+9]), 16, 32)
					if err != nil {
						panic(fmt.Errorf("internal parser error: %v", err))
					}
					buf.WriteRune(rune(rc))
					i += 9
					continue
				}
				buf.WriteByte(c)
			default:
				buf.WriteRune(r[i])
			}
			i++
		}
		val = buf.String()
	} else {
		val = string(r)
	}
	if len(sp) == 0 {
		if isQuoted {
			return quad.String(val)
		}
		return quad.Raw(val)
	}
	if sp[0] == '@' {
		return quad.LangString{
			Value: quad.String(val),
			Lang:  string(sp[1:]),
		}
	} else if len(sp) >= 4 && sp[0] == '^' && sp[1] == '^' && sp[2] == '<' && sp[len(sp)-1] == '>' {
		v := quad.TypedString{
			Value: quad.String(val),
			Type:  quad.IRI(sp[3 : len(sp)-1]),
		}
		if AutoConvertTypedString {
			nv, err := v.ToNative()
			if err == nil {
				return nv
			}
		}
		return v
	}
	return quad.Raw(raw)
}

// NewEncoder returns an N-Quad encoder that writes its output to the
// provided io.Writer.
func NewEncoder(w io.Writer) *Encoder { return &Encoder{w: w} }

// Encoder implements N-Quad document generator according to the RDF
// 1.1 N-Quads specification.
type Encoder struct {
	w   io.Writer
	err error
}

func (enc *Encoder) writeValue(s string) {
	if enc.err != nil {
		return
	}
	_, enc.err = enc.w.Write([]byte(s + " "))
}
func (enc *Encoder) WriteQuad(q quad.Quad) error {
	enc.writeValue(quad.StringOf(q.Subject))
	enc.writeValue(quad.StringOf(q.Predicate))
	enc.writeValue(quad.StringOf(q.Object))
	if q.Label != nil {
		enc.writeValue(quad.StringOf(q.Label))
	}
	if enc.err != nil {
		return enc.err
	}
	_, enc.err = enc.w.Write([]byte(".\n"))
	return enc.err
}
func (enc *Encoder) Close() error { return enc.err }
