package main

import (
	"compress/gzip"
	"flag"
	"fmt"
	"github.com/google/cayley/quad"
	_ "github.com/google/cayley/quad/cquads"
	_ "github.com/google/cayley/quad/gml"
	_ "github.com/google/cayley/quad/graphml"
	_ "github.com/google/cayley/quad/json"
	_ "github.com/google/cayley/quad/jsonld"
	_ "github.com/google/cayley/quad/pquads"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type writeHook struct {
	w quad.Writer
	f func()
}

func (w writeHook) WriteQuad(q quad.Quad) error {
	if err := w.w.WriteQuad(q); err != nil {
		return err
	}
	w.f()
	return nil
}

func main() {
	flag.Parse()
	args := flag.Args()
	if len(args) < 2 {
		fmt.Println("usage: qconv <src> <dst>")
		os.Exit(1)
	}
	start := time.Now()
	var qw quad.WriteCloser
	var cnt int
	{
		name := args[len(args)-1]
		args = args[:len(args)-1]
		file, err := os.Create(name)
		if err != nil {
			log.Fatal(err)
		}
		defer file.Close()
		var w io.Writer = file
		ext := filepath.Ext(name)
		if strings.HasSuffix(ext, ".gz") {
			ext = filepath.Ext(strings.TrimSuffix(name, ".gz"))
			gz := gzip.NewWriter(w)
			defer gz.Close()
			w = gz
		}
		f := quad.FormatByExt(ext)
		if f == nil || f.Writer == nil {
			log.Fatal("unknown extension:", ext)
		}
		qw = f.Writer(w)
	}
	defer qw.Close()
	var errored bool
	for _, name := range args {
		err := func() error {
			var qr quad.ReadCloser
			file, err := os.Open(name)
			if err != nil {
				return err
			}
			defer file.Close()

			var r io.Reader = file
			ext := filepath.Ext(name)
			if strings.HasSuffix(ext, ".gz") {
				ext = filepath.Ext(strings.TrimSuffix(name, ".gz"))
				gz, err := gzip.NewReader(r)
				if err != nil {
					return err
				}
				defer gz.Close()
				r = gz
			}
			f := quad.FormatByExt(ext)
			if f == nil || f.Reader == nil {
				return fmt.Errorf("unknown extension: %v", ext)
			}
			qr = f.Reader(r)
			defer qr.Close()
			_, err = quad.Copy(writeHook{w: qw, f: func() {
				cnt++
				if cnt%(1000*1000) == 0 {
					log.Printf("written %dM quads in %v", cnt/1000/1000, time.Since(start))
				}
			}}, qr)
			return err
		}()
		if err != nil {
			log.Println("error:", err)
			errored = true
			continue
		}
	}
	log.Printf("written %d quads in %v", cnt, time.Since(start))
	if errored {
		os.Exit(1)
	}
}
