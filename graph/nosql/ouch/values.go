package ouch

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/cayleygraph/cayley/graph/nosql"
)

const (
	int64Adjust  = 1 << 63
	keySeparator = "|"
	timeFormat   = time.RFC3339Nano // seconds resolution only without Nano
)

// itos serializes int64 into a sortable string 13 chars long.
// NOTE: in JS there are no native 64bit integers.
func itos(i int64) string {
	s := strconv.FormatUint(uint64(i)+int64Adjust, 32)
	const z = "0000000000000"
	return z[len(s):] + s
}

// stoi de-serializes int64 from a sortable string 13 chars long.
func stoi(s string) int64 {
	ret, err := strconv.ParseUint(s, 32, 64)
	if err != nil {
		//TODO handle error?
		return 0
	}
	return int64(ret - int64Adjust)
}

// toInterfaceValue serializes nosql.Value -> native json values.
func toInterfaceValue(k string, v nosql.Value) interface{} {
	switch v := v.(type) {
	case nil:
		return nil
	case nosql.Document:
		//fmt.Println("toInterfaceValue Doc", v)
		// see nodeValue in ../quadstore.go
		return toInterfaceDoc(v)
	case nosql.Array: // TODO this encoding of array may be problematic, as we've lost the type info
		arr := make([]interface{}, 0, len(v))
		for _, s := range v {
			arr = append(arr, toInterfaceValue(k, s))
		}
		return arr
	case nosql.Key: // special handling here, as type can't be inferred from json
		return "K" + strings.Join(v, keySeparator)
	case nosql.String:
		if k[0] == '_' {
			return string(v)
		}
		return "S" + string(v) // need leading "S"
	case nosql.Int: // special handling here, as type can't be inferred from json
		return "I" + itos(int64(v))
	case nosql.Float:
		return float64(v)
	case nosql.Bool:
		return bool(v)
	case nosql.Time: // special handling here, as type can't be inferred from json
		ret := "T" + time.Time(v).Format(timeFormat)
		//fmt.Println("DEBUG " + ret)
		return ret
	case nosql.Bytes: // special handling here, as type can't be inferred from json
		return "B" + base64.StdEncoding.EncodeToString(v)
	default:
		panic(fmt.Errorf("unsupported type: %T", v))
	}
}

func toInterfaceDoc(d nosql.Document) map[string]interface{} {
	if d == nil {
		return nil
	}
	m := make(map[string]interface{})
	for k, v := range d {
		m[k] = toInterfaceValue(k, v)
	}

	// roundtriptest TODO remove!
	b := fromInterfaceDoc(m)
	if err := dcompare(d, b); err != nil {
		fmt.Printf("DEBUG round-trip failed error: %v\nout: %#v\nback: %#v\n", err, d, b)
	}

	return m
}

func fromInterfaceValue(k string, v interface{}) nosql.Value {
	switch v := v.(type) {
	case nil:
		return nil
	case map[string]interface{}:
		//fmt.Println("fromInterfaceValue Doc", v)
		// see nodeValue in ../quadstore.go
		return fromInterfaceDoc(v)
	case []interface{}:
		arr := make(nosql.Array, 0, len(v))
		for _, s := range v {
			arr = append(arr, fromInterfaceValue(k, s))
		}
		return arr
	case string:
		if len(v) == 0 {
			return nosql.String("")
		}
		if k[0] == '_' && k != idField {
			return nosql.String(v)
		}
		typ := v[0]
		v = v[1:]
		switch typ {
		case 'S':
			return nosql.String(v)

		case 'K':
			parts := strings.Split(v, keySeparator)
			key := make(nosql.Key, 0, len(parts))
			for _, part := range parts {
				key = append(key, part)
			}
			return key

		case 'B':
			byts, err := base64.StdEncoding.DecodeString(v)
			if err != nil {
				// TODO consider how to handle this error properly
				return nosql.Bytes(nil)
			}
			return nosql.Bytes(byts)

		case 'T':
			var time0 nosql.Time
			tim, err := time.Parse(timeFormat, v)
			if err != nil {
				// TODO consider how to handle this error properly
				fmt.Println("DEBUG Time parse", tim, err)
				return time0
			}
			return nosql.Time(tim)

		case 'I':
			return nosql.Int(stoi(v))

		default:
			panic(fmt.Errorf("unsupported serialized type: %v", v))
		}
	case int:
		return nosql.Int(v)
	case int64:
		return nosql.Int(v)
	case float64:
		return nosql.Float(v)
	case bool:
		return nosql.Bool(v)
	case time.Time:
		return nosql.Time(v)
	case []byte:
		return nosql.Bytes(v)
	default:
		panic(fmt.Errorf("unsupported type: %T", v))
	}
}

func fromInterfaceDoc(d map[string]interface{}) nosql.Document {
	if d == nil {
		return nil
	}
	m := make(nosql.Document, len(d))
	for k, v := range d {
		m[k] = fromInterfaceValue(k, v)
	}
	return m
}
