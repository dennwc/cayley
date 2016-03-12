package turtle

import "strings"

func ParseTerm(s string) Term {
	if len(s) <= 2 {
		return Raw(s)
	}
	// TODO(dennwc): parse for real
	if s[0] == '<' && s[len(s)-1] == '>' {
		return IRI(s[1 : len(s)-1])
	} else if s[0] == '"' {
		if s[len(s)-1] == '"' {
			return Literal{
				Value: Unescape(s[1 : len(s)-1]),
			}
		} else if i := strings.Index(s, `"^^<`); s[len(s)-1] == '>' && i > 0 {
			return Literal{
				Value:    Unescape(s[1:i]),
				DataType: IRI(s[i+4 : len(s)-1]),
			}
		} else if i = strings.Index(s, `"@`); i > 0 {
			return Literal{
				Value:    Unescape(s[1:i]),
				Language: s[i+2:],
			}
		}
		return Raw(s)
	} else if strings.Index(s, "_:") == 0 {
		return BlankNode(s[2:])
	}
	return Raw(s)
}

type Term interface {
	String() string
}

type Literal struct {
	Value    string
	Language string
	DataType IRI
}

func (v Literal) String() string {
	if v.Value == "" {
		return ""
	}
	s := `"` + Escape(v.Value) + `"`
	if v.Language != "" {
		s += "@" + v.Language
	} else if v.DataType != "" {
		s += "^^" + v.DataType.String()
	}
	return s
}

type IRI string

func (s IRI) String() string {
	if s == "" {
		return ""
	}
	return "<" + string(s) + ">"
}

type BlankNode string

func (s BlankNode) String() string {
	if s == "" {
		return ""
	}
	return "_:" + string(s)
}

type Raw string

func (s Raw) String() string { return string(s) }

var (
	escaper = strings.NewReplacer(
		"\\", "\\\\",
		"\"", "\\\"",
		"\n", "\\n",
		"\r", "\\r",
		"\t", "\\t",
	)
	unescaper = strings.NewReplacer(
		"\\\\", "\\",
		"\\\"", "\"",
		"\\n", "\n",
		"\\r", "\r",
		"\\t", "\t",
	)
)

func Escape(s string) string {
	return escaper.Replace(s)
}

func Unescape(s string) string {
	return unescaper.Replace(s)
}
