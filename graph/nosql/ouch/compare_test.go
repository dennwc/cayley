package ouch

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"time"

	"github.com/cayleygraph/cayley/graph/nosql"
	"github.com/gopherjs/gopherjs/js"
)

// debug a mango query string to be generated - not used by tests directly, just as a temporary debug aid from the main code.
func (q *Query) debug(findStr string) {
	if runtime.GOARCH == "js" {
		qry := js.Global.Get("JSON").Call("stringify", q.ouchQuery).String()
		if strings.Contains(qry, findStr) {
			fmt.Println("DEBUG mango query marshal JS", qry)
		}
	} else {
		byts, err := json.Marshal(q.ouchQuery)
		qry := string(byts)
		if strings.Contains(qry, findStr) {
			fmt.Println("DEBUG mango query marshal", err, qry)
		}
	}
}

// checkDoc is what we expect it to be - used by tests, can also be used as debug from main code during testing.
func (db *DB) checkDoc(col string, key nosql.Key, d nosql.Document) error {
	decoded, err := db.FindByKey(context.TODO(), col, key)

	if err != nil {
		return fmt.Errorf("unable to find record to check FindByKey '%v' error: %v", key, err)
	}

	return dcompare(decoded, d)
}

// dcompare() writen like this so that it can be called as a debug aid in the main code during testing too.
// comment from @dennwc - you can use require.Equal (it prints diff) for tests and reflect.DeepEqual + fmt.Sprintf("%#v") for the package
func dcompare(rt, d nosql.Document) error {

	seen := make(map[string]bool)
	for k, v := range d {
		seen[k] = true
		switch k {
		case collectionField, idField, revField:
			//ignore
		default:
			if !reflect.DeepEqual(v, rt[k]) {
				tim, isT := v.(nosql.Time)
				if isT {
					if !time.Time(tim).Equal(time.Time(rt[k].(nosql.Time))) { // this will panic if not time
						return fmt.Errorf("times not equal for %#v %#v %#v\n", k, v, rt[k])
					}
				} else {
					return fmt.Errorf("not equal %#v %#v %#v\n", k, v, rt[k])
				}
			}
		}
	}
	for k, v := range rt { // now look from the other direction
		if !seen[k] {
			switch k {
			case collectionField, idField, revField:
				//ignore
			default:
				tim, isT := v.(nosql.Time)
				if isT {
					if !time.Time(tim).Equal(time.Time(d[k].(nosql.Time))) { // this will panic if not time
						return fmt.Errorf("times not equal (unseen) for %#v %#v %#v\n", k, v, d[k])
					}
				} else {
					if !reflect.DeepEqual(v, d[k]) {
						return fmt.Errorf("not equal (unseen) %#v %#v %#v\n", k, v, d[k])
					}
				}
			}
		}
	}

	return nil
}
