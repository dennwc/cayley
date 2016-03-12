package turtle

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
)

//go:generate ragel -Z -G2 parse.rl

var ErrInvalid = fmt.Errorf("invalid turtle value")

type Term interface {
	String() string
}

type String string

func (v String) String() string {
	return `"` + Escape(string(v)) + `"`
}

type LangString struct {
	Value String
	Lang  string
}

func (v LangString) String() string {
	return v.Value.String() + "@" + v.Lang
}

type TypedString struct {
	Value String
	Type  IRI
}

func (v TypedString) String() string {
	return v.Value.String() + "^^" + v.Type.String()
}

type IRI string

func (s IRI) String() string {
	return "<" + string(s) + ">"
}

type BlankNode string

func (s BlankNode) String() string {
	return "_:" + string(s)
}

type Raw string

func (s Raw) String() string { return string(s) }

var escaper = strings.NewReplacer(
	"\\", "\\\\",
	"\"", "\\\"",
	"\n", "\\n",
	"\r", "\\r",
	"\t", "\\t",
)

func Escape(s string) string {
	return escaper.Replace(s)
}

func Unescape(s string) string {
	return unEscape([]rune(s))
}

func unEscape(r []rune) string {
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
	return buf.String()
}
