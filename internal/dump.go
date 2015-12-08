package internal

import (
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/google/cayley/graph"
	"github.com/google/cayley/quad"

	// Register all supported formats for encoding
	_ "github.com/google/cayley/quad/gml"
	_ "github.com/google/cayley/quad/graphml"
	_ "github.com/google/cayley/quad/json"
	_ "github.com/google/cayley/quad/jsonld"
	_ "github.com/google/cayley/quad/nquads"
)

// Dump the content of the database into a file based
// on a few different formats
func Dump(qs graph.QuadStore, outFile, typ string) error {
	var f *os.File
	if outFile == "-" {
		f = os.Stdout
	} else {
		var err error
		f, err = os.Create(outFile)
		if err != nil {
			return fmt.Errorf("could not open file %q: %v", outFile, err)
		}
		defer f.Close()
		fmt.Printf("dumping db to file %q\n", outFile)
	}

	var w io.Writer = f
	if filepath.Ext(outFile) == ".gz" {
		gzip := gzip.NewWriter(f)
		defer gzip.Close()
		w = gzip
	}
	qr := graph.NewQuadReader(qs) //TODO: add possible support for exporting specific queries only

	if typ == "quad" { // compatibility
		typ = "nquads"
	}
	format := quad.FormatByName(typ)
	if format == nil {
		return fmt.Errorf("unknown format %q", typ)
	} else if format.Writer == nil {
		return fmt.Errorf("format %q: encoding is not supported", typ)
	}
	qw := format.Writer(w)
	count, err := quad.Copy(qw, qr)
	if err != nil {
		qw.Close()
		return err
	}
	if err = qw.Close(); err != nil {
		return err
	}
	if outFile != "-" {
		fmt.Printf("%d entries were written\n", count)
	}
	return nil
}
