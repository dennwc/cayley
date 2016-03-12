package main

import (
	"compress/gzip"
	"flag"
	"fmt"
	"github.com/google/cayley/quad"
	"github.com/google/cayley/quad/cquads"
	"github.com/google/cayley/quad/pquads"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type quadReader struct {
	r interface {
		Unmarshal() (quad.Quad, error)
	}
}

func (r quadReader) ReadQuad() (quad.Quad, error) {
	return r.r.Unmarshal()
}

type nquadsWriter struct {
	w io.Writer
}

func (w nquadsWriter) WriteQuad(q quad.Quad) error {
	_, err := fmt.Fprintln(w.w, q.NQuad())
	return err
}

func main() {
	flag.Parse()
	args := flag.Args()
	if len(args) < 2 {
		fmt.Println("usage: qconv <src> <dst>")
		os.Exit(1)
	}
	start := time.Now()
	var qw interface {
		WriteQuad(q quad.Quad) error
	}
	var cnt int64
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
		switch ext {
		case ".pq":
			qw = pquads.NewWriter(w)
		case ".nq":
			log.Println("WARNING: nquads parser has bad escaping")
			qw = nquadsWriter{w}
		default:
			log.Fatal("unknown extension:", ext)
		}
	}
	var errored bool
	for _, name := range args {
		err := func() error {
			var qr interface {
				ReadQuad() (quad.Quad, error)
			}
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
			switch ext {
			case ".pq":
				qr = pquads.NewReader(r)
			case ".nq":
				qr = quadReader{cquads.NewDecoder(r)}
			default:
				return fmt.Errorf("unknown extension: %v", ext)
			}
			for {
				q, err := qr.ReadQuad()
				if err == io.EOF {
					return nil
				} else if err != nil {
					return fmt.Errorf("failed to read quad: %v", err)
				}
				if err = qw.WriteQuad(q); err != nil {
					return fmt.Errorf("failed to write quad: %v", err)
				}
				cnt++
				if cnt%(1000*1000) == 0 {
					log.Printf("written %dM quads in %v", cnt/1000/1000, time.Since(start))
				}
			}
			return nil
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
