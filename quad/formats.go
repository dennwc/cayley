package quad

import (
	"fmt"
	"io"
)

// Format is a description for quad-file formats.
type Format struct {
	// Name is a short format name used as identifier for RegisterFormat.
	Name string
	// Ext is a list of file extensions, allowed for file format. Can be used to detect file format, given a path.
	Ext []string
	// Mime is a list of MIME (content) types, allowed for file format. Can be used in HTTP request/responses.
	Mime []string
	// Reader is a function for creating format reader, that reads serialized data from io.Reader.
	Reader func(io.Reader) ReadCloser
	// Writer is a function for creating format writer, that streams serialized data to io.Writer.
	Writer func(io.Writer) WriteCloser
}

var (
	formatsByName = make(map[string]*Format)
)

// RegisterFormat registers a new quad-file format.
func RegisterFormat(f Format) {
	_, ok := formatsByName[f.Name]
	if ok {
		panic(fmt.Errorf("format %s is allready registered", f.Name))
	}
	formatsByName[f.Name] = &f
}

// FormatByName returns a registered format by its name. Will return nil if format is not found.
func FormatByName(name string) *Format {
	return formatsByName[name]
}
