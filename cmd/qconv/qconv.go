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

type devNull struct{}

func (w devNull) WriteQuad(q quad.Quad) error { return nil }
func (w devNull) Close() error                { return nil }

type quadHook struct {
	w quad.WriteCloser
	f []func(q quad.Quad)
}

func (w quadHook) WriteQuad(q quad.Quad) error {
	for _, f := range w.f {
		f(q)
	}
	return w.w.WriteQuad(q)
}
func (w quadHook) Close() error { return w.w.Close() }

var (
	f_drop  = flag.Bool("drop", false, "do not write quads to output, read providen files only")
	f_types = flag.Bool("types", false, "print all basic types found in dataset")
	f_lang  = flag.Bool("lang", false, "print all languages found in dataset")
)

func main() {
	flag.Parse()
	args := flag.Args()
	if (!*f_drop && len(args) < 2) || (*f_drop && len(args) < 1) {
		fmt.Println("usage: qconv <src> <dst>")
		os.Exit(1)
	}
	start := time.Now()
	var cnt int
	defer func() {
		log.Printf("written %d quads in %v", cnt, time.Since(start))
	}()
	var qw quad.WriteCloser
	if *f_drop {
		qw = devNull{}
	} else {
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

	var qh []func(q quad.Quad)
	if *f_types {
		m := make(map[quad.IRI]struct{})
		check := func(v quad.Value) {
			if t, ok := v.(quad.TypedString); ok {
				if _, ok = m[t.Type]; !ok {
					log.Println("new type found:", t.Type)
					m[t.Type] = struct{}{}
				}
			}
		}
		defer func() {
			log.Printf("dataset uses %d basic types:\n%v", len(m), m)
		}()
		qh = append(qh, func(q quad.Quad) {
			check(q.Subject)
			check(q.Predicate)
			check(q.Object)
		})
	}
	if *f_lang {
		m := make(map[string]struct{})
		check := func(v quad.Value) {
			if t, ok := v.(quad.LangString); ok {
				if _, ok = m[t.Lang]; !ok {
					log.Println("new language found:", t.Lang)
					m[t.Lang] = struct{}{}
				}
			}
		}
		defer func() {
			log.Printf("dataset uses %d languages:\n%v", len(m), m)
		}()
		qh = append(qh, func(q quad.Quad) {
			check(q.Subject)
			check(q.Predicate)
			check(q.Object)
		})
	}
	if qh != nil {
		qw = quadHook{w: qw, f: qh}
	}

	var errored bool
	for _, name := range args {
		if name == "" || name == "-" {
			continue
		}
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
	if errored {
		os.Exit(1)
	}
}
