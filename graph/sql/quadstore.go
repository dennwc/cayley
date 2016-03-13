package sql

import (
	"database/sql"
	"encoding/hex"
	"fmt"

	"github.com/lib/pq"

	"github.com/barakmich/glog"
	"github.com/google/cayley/graph"
	"github.com/google/cayley/graph/iterator"
	"github.com/google/cayley/graph/proto"
	"github.com/google/cayley/quad"
)

const QuadStoreType = "sql"

func init() {
	graph.RegisterQuadStore(QuadStoreType, graph.QuadStoreRegistration{
		NewFunc:           newQuadStore,
		NewForRequestFunc: nil,
		UpgradeFunc:       nil,
		InitFunc:          createSQLTables,
		IsPersistent:      true,
	})
}

type NodeHash sql.NullString

func (NodeHash) IsNode() bool { return true }

type QuadHashes [4]sql.NullString

func (QuadHashes) IsNode() bool { return false }
func (q QuadHashes) Get(d quad.Direction) sql.NullString {
	switch d {
	case quad.Subject:
		return q[0]
	case quad.Predicate:
		return q[1]
	case quad.Object:
		return q[2]
	case quad.Label:
		return q[3]
	}
	panic(fmt.Errorf("unknown direction: %v", d))
}

type QuadStore struct {
	db           *sql.DB
	sqlFlavor    string
	size         int64
	lru          *cache
	noSizes      bool
	useEstimates bool
}

func connectSQLTables(addr string, _ graph.Options) (*sql.DB, error) {
	// TODO(barakmich): Parse options for more friendly addr, other SQLs.
	conn, err := sql.Open("postgres", addr)
	if err != nil {
		glog.Errorf("Couldn't open database at %s: %#v", addr, err)
		return nil, err
	}
	// "Open may just validate its arguments without creating a connection to the database."
	// "To verify that the data source name is valid, call Ping."
	// Source: http://golang.org/pkg/database/sql/#Open
	if err := conn.Ping(); err != nil {
		glog.Errorf("Couldn't open database at %s: %#v", addr, err)
		return nil, err
	}
	return conn, nil
}

func createSQLTables(addr string, options graph.Options) error {
	conn, err := connectSQLTables(addr, options)
	if err != nil {
		return err
	}
	defer conn.Close()
	tx, err := conn.Begin()
	if err != nil {
		glog.Errorf("Couldn't begin creation transaction: %s", err)
		return err
	}

	table, err := tx.Exec(`
	CREATE TABLE nodes (
		hash TEXT PRIMARY KEY,
		value BYTEA
	);`)
	if err != nil {
		tx.Rollback()
		errd := err.(*pq.Error)
		if errd.Code == "42P07" {
			return graph.ErrDatabaseExists
		}
		glog.Errorf("Cannot create nodes table: %v", table)
		return err
	}
	table, err = tx.Exec(`
	CREATE TABLE quads (
		horizon BIGSERIAL PRIMARY KEY,
		subject_hash TEXT NOT NULL REFERENCES nodes (hash),
		predicate_hash TEXT NOT NULL REFERENCES nodes (hash),
		object_hash TEXT NOT NULL REFERENCES nodes (hash),
		label_hash TEXT REFERENCES nodes (hash),
		id BIGINT,
		ts timestamp
	);`)
	if err != nil {
		tx.Rollback()
		errd := err.(*pq.Error)
		if errd.Code == "42P07" {
			return graph.ErrDatabaseExists
		}
		glog.Errorf("Cannot create quad table: %v", table)
		return err
	}
	factor, factorOk, err := options.IntKey("db_fill_factor")
	if !factorOk {
		factor = 50
	}
	var index sql.Result

	index, err = tx.Exec(fmt.Sprintf(`
	CREATE UNIQUE INDEX spol_unique ON quads (subject_hash, predicate_hash, object_hash, label_hash) WHERE label_hash IS NOT NULL;
	CREATE UNIQUE INDEX spo_unique ON quads (subject_hash, predicate_hash, object_hash) WHERE label_hash IS NULL;
	CREATE INDEX spo_index ON quads (subject_hash) WITH (FILLFACTOR = %d);
	CREATE INDEX pos_index ON quads (predicate_hash) WITH (FILLFACTOR = %d);
	CREATE INDEX osp_index ON quads (object_hash) WITH (FILLFACTOR = %d);
	`, factor, factor, factor))
	if err != nil {
		glog.Errorf("Cannot create indices: %v", index)
		tx.Rollback()
		return err
	}
	tx.Commit()
	return nil
}

func newQuadStore(addr string, options graph.Options) (graph.QuadStore, error) {
	var qs QuadStore
	conn, err := connectSQLTables(addr, options)
	if err != nil {
		return nil, err
	}
	localOpt, localOptOk, err := options.BoolKey("local_optimize")
	if err != nil {
		return nil, err
	}
	qs.db = conn
	qs.sqlFlavor = "postgres"
	qs.size = -1
	qs.lru = newCache(1024)

	// Skip size checking by default.
	qs.noSizes = true
	if localOptOk {
		if localOpt {
			qs.noSizes = false
		}
	}
	qs.useEstimates, _, err = options.BoolKey("use_estimates")
	if err != nil {
		return nil, err
	}

	return &qs, nil
}

func hashOf(s quad.Value) sql.NullString {
	if s == nil {
		return sql.NullString{Valid: false}
	}
	return sql.NullString{Valid: true, String: hex.EncodeToString(quad.HashOf(s))}
}

func convInsertError(err error) error {
	if err == nil {
		return err
	}
	if pe, ok := err.(*pq.Error); ok {
		if pe.Code == "23505" {
			return graph.ErrQuadExists
		}
	}
	return err
}

func marshalQuadDirections(q quad.Quad) (s, p, o, l []byte, err error) {
	s, err = proto.MarshalValue(q.Subject)
	if err != nil {
		return
	}
	p, err = proto.MarshalValue(q.Predicate)
	if err != nil {
		return
	}
	o, err = proto.MarshalValue(q.Object)
	if err != nil {
		return
	}
	l, err = proto.MarshalValue(q.Label)
	if err != nil {
		return
	}
	return
}

func unmarshalQuadDirections(s, p, o, l []byte) (q quad.Quad, err error) {
	q.Subject, err = proto.UnmarshalValue(s)
	if err != nil {
		return
	}
	q.Predicate, err = proto.UnmarshalValue(p)
	if err != nil {
		return
	}
	q.Object, err = proto.UnmarshalValue(o)
	if err != nil {
		return
	}
	q.Label, err = proto.UnmarshalValue(l)
	if err != nil {
		return
	}
	return
}

func unmarshalValue(data sql.NullString) quad.Value {
	if !data.Valid {
		return nil
	}
	v, err := proto.UnmarshalValue([]byte(data.String))
	if err != nil {
		glog.Errorf("couldn't unmarshal value: %v", err)
		return nil
	}
	return v
}

func (qs *QuadStore) copyFrom(tx *sql.Tx, in []graph.Delta, opts graph.IgnoreOpts) error {
	panic("broken")
	stmt, err := tx.Prepare(pq.CopyIn("quads", "subject", "predicate", "object", "label", "id", "ts", "subject_hash", "predicate_hash", "object_hash", "label_hash"))
	if err != nil {
		glog.Errorf("couldn't prepare COPY statement: %v", err)
		return err
	}
	for _, d := range in {
		s, p, o, l, err := marshalQuadDirections(d.Quad)
		if err != nil {
			glog.Errorf("couldn't marshal quads: %v", err)
			return err
		}
		_, err = stmt.Exec(
			s,
			p,
			o,
			l,
			d.ID.Int(),
			d.Timestamp,
			hashOf(d.Quad.Subject),
			hashOf(d.Quad.Predicate),
			hashOf(d.Quad.Object),
			hashOf(d.Quad.Label),
		)
		if err != nil {
			err = convInsertError(err)
			glog.Errorf("couldn't execute COPY statement: %v", err)
			return err
		}
	}
	//if _, err = stmt.Exec(); err != nil {
	//	glog.Errorf("couldn't execute COPY statement 2: %v", err)
	//	return err
	//}
	if err = stmt.Close(); err != nil {
		glog.Errorf("couldn't close COPY statement: %v", err)
		err = convInsertError(err)
		return err
	}
	return nil
}

func (qs *QuadStore) runTxPostgres(tx *sql.Tx, in []graph.Delta, opts graph.IgnoreOpts) error {
	//allAdds := true
	//for _, d := range in {
	//	if d.Action != graph.Add {
	//		allAdds = false
	//	}
	//}
	//if allAdds && !opts.IgnoreDup {
	//	return qs.copyFrom(tx, in, opts)
	//}

	for _, d := range in {
		switch d.Action {
		case graph.Add:
			end := ";"
			if opts.IgnoreDup {
				end = " ON CONFLICT DO NOTHING;"
			}
			var hs, hp, ho, hl sql.NullString
			for _, dir := range quad.Directions {
				v := d.Quad.Get(dir)
				if v == nil {
					continue
				}
				h := hashOf(v)
				switch dir {
				case quad.Subject:
					hs = h
				case quad.Predicate:
					hp = h
				case quad.Object:
					ho = h
				case quad.Label:
					hl = h
				}
				p, err := proto.MarshalValue(v)
				if err != nil {
					glog.Errorf("couldn't marshal value: %v", err)
					return err
				}
				_, err = tx.Exec(`INSERT INTO nodes(hash, value) VALUES ($1, $2) ON CONFLICT DO NOTHING;`,
					h, p,
				)
				err = convInsertError(err)
				if err != nil {
					glog.Errorf("couldn't exec INSERT statement: %v", err)
					return err
				}
			}
			_, err := tx.Exec(`INSERT INTO quads(subject_hash, predicate_hash, object_hash, label_hash, id, ts) VALUES ($1, $2, $3, $4, $5, $6)`+end,
				hs, hp, ho, hl,
				d.ID.Int(),
				d.Timestamp,
			)
			err = convInsertError(err)
			if err != nil {
				glog.Errorf("couldn't exec INSERT statement: %v", err)
				return err
			}
		case graph.Delete:
			var (
				result sql.Result
				err    error
			)
			if d.Quad.Label == nil {
				result, err = tx.Exec(`DELETE FROM quads WHERE subject_hash=$1 and predicate_hash=$2 and object_hash=$3 and label_hash is null;`,
					hashOf(d.Quad.Subject), hashOf(d.Quad.Predicate), hashOf(d.Quad.Object))
			} else {
				result, err = tx.Exec(`DELETE FROM quads WHERE subject_hash=$1 and predicate_hash=$2 and object_hash=$3 and label_hash=$4;`,
					hashOf(d.Quad.Subject), hashOf(d.Quad.Predicate), hashOf(d.Quad.Object), hashOf(d.Quad.Label))
			}
			if err != nil {
				glog.Errorf("couldn't exec DELETE statement: %v", err)
				return err
			}
			affected, err := result.RowsAffected()
			if err != nil {
				glog.Errorf("couldn't get DELETE RowsAffected: %v", err)
				return err
			}
			if affected != 1 && !opts.IgnoreMissing {
				return graph.ErrQuadNotExist
			}
		default:
			panic("unknown action")
		}
	}
	qs.size = -1 // TODO(barakmich): Sync size with writes.
	return nil
}

func (qs *QuadStore) ApplyDeltas(in []graph.Delta, opts graph.IgnoreOpts) error {
	tx, err := qs.db.Begin()
	if err != nil {
		glog.Errorf("couldn't begin write transaction: %v", err)
		return err
	}
	switch qs.sqlFlavor {
	case "postgres":
		err = qs.runTxPostgres(tx, in, opts)
		if err != nil {
			tx.Rollback()
			return err
		}
	default:
		panic("no support for flavor: " + qs.sqlFlavor)
	}
	return tx.Commit()
}

func (qs *QuadStore) Quad(val graph.Value) quad.Quad {
	h := val.(QuadHashes)
	query := `SELECT
		(SELECT value FROM nodes WHERE hash = $1 LIMIT 1) AS subject,
		(SELECT value FROM nodes WHERE hash = $2 LIMIT 1) AS predicate,
		(SELECT value FROM nodes WHERE hash = $3 LIMIT 1) AS object,
		(SELECT value FROM nodes WHERE hash = $4 LIMIT 1) AS label
		;`
	c := qs.db.QueryRow(query, h[0], h[1], h[2], h[3])
	var s, p, o, l []byte
	if err := c.Scan(&s, &p, &o, &l); err != nil {
		glog.Errorf("Couldn't execute quad values lookup: %v", err)
		return quad.Quad{}
	}
	q, err := unmarshalQuadDirections(s, p, o, l)
	if err != nil {
		glog.Errorf("Couldn't unmarshal quad: %v", err)
		return quad.Quad{}
	}
	return q
}

func (qs *QuadStore) QuadIterator(d quad.Direction, val graph.Value) graph.Iterator {
	return newSQLLinkIterator(qs, d, val.(NodeHash))
}

func (qs *QuadStore) NodesAllIterator() graph.Iterator {
	return NewAllIterator(qs, "nodes")
}

func (qs *QuadStore) QuadsAllIterator() graph.Iterator {
	return NewAllIterator(qs, "quads")
}

func (qs *QuadStore) ValueOf(s quad.Value) graph.Value {
	return NodeHash(hashOf(s))
}

func (qs *QuadStore) NameOf(v graph.Value) quad.Value {
	if v == nil {
		glog.V(2).Info("NameOf was nil")
		return nil
	}
	hash := v.(NodeHash)
	query := "SELECT value FROM nodes WHERE hash = $1 LIMIT 1;"
	c := qs.db.QueryRow(query, sql.NullString(hash))
	var data []byte
	if err := c.Scan(&data); err != nil {
		glog.Errorf("Couldn't execute value lookup: %v", err)
		return nil
	}
	qv, err := proto.UnmarshalValue(data)
	if err != nil {
		glog.Errorf("Couldn't unmarshal value: %v", err)
		return nil
	}
	return qv
}

func (qs *QuadStore) Size() int64 {
	if qs.size != -1 {
		return qs.size
	}

	query := "SELECT COUNT(*) FROM quads;"
	if qs.useEstimates {
		switch qs.sqlFlavor {
		case "postgres":
			query = "SELECT reltuples::BIGINT AS estimate FROM pg_class WHERE relname='quads';"
		default:
			panic("no estimate support for flavor: " + qs.sqlFlavor)
		}
	}

	c := qs.db.QueryRow(query)
	err := c.Scan(&qs.size)
	if err != nil {
		glog.Errorf("Couldn't execute COUNT: %v", err)
		return 0
	}
	return qs.size
}

func (qs *QuadStore) Horizon() graph.PrimaryKey {
	var horizon int64
	err := qs.db.QueryRow("SELECT horizon FROM quads ORDER BY horizon DESC LIMIT 1;").Scan(&horizon)
	if err != nil {
		if err != sql.ErrNoRows {
			glog.Errorf("Couldn't execute horizon: %v", err)
		}
		return graph.NewSequentialKey(0)
	}
	return graph.NewSequentialKey(horizon)
}

func (qs *QuadStore) FixedIterator() graph.FixedIterator {
	return iterator.NewFixed(iterator.Identity)
}

func (qs *QuadStore) Close() {
	qs.db.Close()
}

func (qs *QuadStore) QuadDirection(in graph.Value, d quad.Direction) graph.Value {
	return NodeHash(in.(QuadHashes).Get(d))
}

func (qs *QuadStore) Type() string {
	return QuadStoreType
}

func (qs *QuadStore) sizeForIterator(isAll bool, dir quad.Direction, hash sql.NullString) int64 {
	var err error
	if isAll {
		return qs.Size()
	}
	if qs.noSizes {
		if dir == quad.Predicate {
			return (qs.Size() / 100) + 1
		}
		return (qs.Size() / 1000) + 1
	}
	if val, ok := qs.lru.Get(hash.String + string(dir.Prefix())); ok {
		return val
	}
	var size int64
	glog.V(4).Infoln("sql: getting size for select %s, %v", dir.String(), hash)
	err = qs.db.QueryRow(
		fmt.Sprintf("SELECT count(*) FROM quads WHERE %s_hash = $1;", dir.String()), hash).Scan(&size)
	if err != nil {
		glog.Errorln("Error getting size from SQL database: %v", err)
		return 0
	}
	qs.lru.Put(hash.String+string(dir.Prefix()), size)
	return size
}
