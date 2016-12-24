package bolt

import (
	"github.com/boltdb/bolt"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestIterator(t *testing.T) {
	b, closer := makeBoltDB(t)
	defer closer()
	kvs := [][2]string{
		{"a", "foo"},
		{"ac", "baz"},
		{"b", "bar"},
	}
	bucket := []byte("test")
	err := b.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(bucket)
		if err != nil {
			return err
		}
		for _, kv := range kvs {
			if err = b.Put([]byte(kv[0]), []byte(kv[1])); err != nil {
				return err
			}
		}
		return nil
	})
	require.Nil(t, err)
	it := NewIterator(b, bucket)
	defer it.Close()
	var out [][2]string
	for it.Next() {
		out = append(out, [2]string{string(it.Key()), string(it.Value())})
	}
	require.Equal(t, kvs, out)
}
