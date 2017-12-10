package ouch

import (
	"encoding/json"
	"fmt"
	"runtime"
	"strings"
	"time"

	"github.com/cayleygraph/cayley/graph/nosql"
	"github.com/gopherjs/gopherjs/js"
)

// TODO remove this whole file and references to it once code is stable.

func trace(x interface{}) {
	fmt.Println("DEBUG trace", x)
}

func (q *Query) debug(findStr string) {
	if runtime.GOARCH == "js" {
		qry := js.Global.Get("JSON").Call("stringify", q.ouchQuery).String()
		if strings.Contains(qry, findStr) {
			fmt.Println("DEBUG query marshal JS", qry)
		}
	} else {
		byts, err := json.Marshal(q.ouchQuery)
		qry := string(byts)
		if strings.Contains(qry, findStr) {
			fmt.Println("DEBUG query marshal", err, qry)
		}
	}
}

func (db *DB) dcheck(col string, key nosql.Key, d nosql.Document) error {
	decoded, err := db.FindByKey(col, key)

	if err != nil {
		return fmt.Errorf("unable to find record to check FindByKey '%v' error: %v", key, err)
	}

	return dcompare(decoded, d)
}

// TODO comment from @dennwc - you can use require.Equal (it prints diff) for tests and reflect.DeepEqual + fmt.Sprintf("%#v") for the package

func dcompare(decoded, d nosql.Document) error {
	for k, v := range decoded {
		if k != "_rev" && !strings.HasSuffix(k, "|bnode") && !strings.HasSuffix(k, "|iri") { // _ fields should have changed
			switch x := v.(type) {
			case nosql.Document:
				if err := dcompare(d[k].(nosql.Document), x); err != nil {
					return err
				}
			case nosql.Strings:
				for i, kv := range x {
					if d[k].(nosql.Strings)[i] != kv {
						return fmt.Errorf("decoded key %s not-equal original %v", k)
					}
				}
			case nosql.Bytes:
				for i, kv := range x {
					if d[k].(nosql.Bytes)[i] != kv {
						return fmt.Errorf("bytes %s not-equal", k)
					}
				}
			case nosql.Time:
				if !time.Time(d[k].(nosql.Time)).Equal(time.Time(x)) {
					return fmt.Errorf("decoded Time value not-equal original %s %v %T %v %T", k, v, v, d[k], d[k])
				}
			default:
				if d[k] != v {
					return fmt.Errorf("decoded value not-equal original %s %v %T %v %T", k, v, v, d[k], d[k])
				}
			}
		}
	}
	for k, v := range d {
		if k[0] != '_' { // _ fields should have changed
			switch x := v.(type) {
			case nosql.Document:
				if err := dcompare(x, decoded[k].(nosql.Document)); err != nil {
					return err
				}
			case nosql.Bytes:
				for i, kv := range x {
					if decoded[k].(nosql.Bytes)[i] != kv {
						return fmt.Errorf("bytes %s not-equal", k)
					}
				}
			case nosql.Strings:
				for i, kv := range x {
					if decoded[k].(nosql.Strings)[i] != kv {
						return fmt.Errorf("keys %s not-equal", k)
					}
				}
			case nosql.Time:
				if !time.Time(decoded[k].(nosql.Time)).Equal(time.Time(x)) {
					return fmt.Errorf("Time value not-equal %s %v %T %v %T", k, v, v, d[k], d[k])
				}

			default:
				if decoded[k] != v {
					return fmt.Errorf("value not-equal %s %v %T %v %T", k, v, v, decoded[k], decoded[k])
				}
			}
		}
	}
	return nil
}
