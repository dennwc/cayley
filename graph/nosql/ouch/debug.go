package ouch

import (
	"fmt"
	"time"

	"github.com/cayleygraph/cayley/graph/nosql"
)

func (db *DB) dcheck(key nosql.Key, d nosql.Document) error {
	col := string(d[collectionField].(nosql.String))
	decoded, err := db.FindByKey(col, key)

	if err != nil {
		return fmt.Errorf("unable to find record to check FindByKey '%v' error: %v", key, err)
	}

	return dcompare(decoded, d)
}

func dcompare(decoded, d nosql.Document) error {
	for k, v := range decoded {
		if k != "_rev" { // _ fields should have changed
			switch x := v.(type) {
			case nosql.Document:
				if err := dcompare(d[k].(nosql.Document), x); err != nil {
					return err
				}
			case nosql.Key:
				for i, kv := range x {
					if d[k].(nosql.Key)[i] != kv {
						return fmt.Errorf("decoded key %s not-equal original %v", k)
					}
				}
			case nosql.Bytes:
				for i, kv := range x {
					if d[k].(nosql.Bytes)[i] != kv {
						return fmt.Errorf("bytes %s not-equal", k)
					}
				}
			case nosql.Array:
				_, ok := d[k].(nosql.Array)
				if !ok {
					return fmt.Errorf("decoded array not-equal original %s %v %T %v %T", k, v, v, d[k], d[k])
				}
				// check contents of arrays
				for i, kv := range x {
					if err := dcompare(
						nosql.Document{k: d[k].(nosql.Array)[i]},
						nosql.Document{k: kv},
					); err != nil {
						return err
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
			case nosql.Key:
				for i, kv := range x {
					if decoded[k].(nosql.Key)[i] != kv {
						return fmt.Errorf("keys %s not-equal", k)
					}
				}
			case nosql.Array: // TODO
				_, ok := decoded[k].(nosql.Array)
				if !ok {
					return fmt.Errorf("array not-equal %s %v %T %v %T", k, v, v, decoded[k], decoded[k])
				}
				for i, kv := range x {
					if err := dcompare(
						nosql.Document{k: decoded[k].(nosql.Array)[i]},
						nosql.Document{k: kv},
					); err != nil {
						return err
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
