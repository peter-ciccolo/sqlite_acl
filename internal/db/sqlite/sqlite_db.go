package sqlite

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/mattn/go-sqlite3"
)

type SQLiteDB struct {
	db *sql.DB
}

func NewSQLiteDB(ctx context.Context, connectionStr string) (*SQLiteDB, error) {
	backing, err := sql.Open("sqlite3", connectionStr)
	if err != nil {
		return nil, err
	}

	return &SQLiteDB{
		db: backing,
	}, nil
}

func (db *SQLiteDB) GetPKs(ctx context.Context) (map[string][]string, error) {
	// Query list of table names
	rows, err := db.db.QueryContext(ctx, "SELECT name FROM sqlite_master WHERE type='table'")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	pks := make(map[string][]string)
	for rows.Next() {
		var tableName string
		pksForTable := make([]string, 0)
		err := rows.Scan(&tableName)
		if err != nil {
			return nil, err
		}

		// For each table, get primary keys
		// SQLite PRAGMA gives columns in cid order, so should match composite key order.
		pkRows, err := db.db.QueryContext(ctx, fmt.Sprintf("PRAGMA table_info(%s)", tableName))
		if err != nil {
			return nil, err
		}

		var cid int
		var name string
		var ttype string
		var notnull int
		var dflt_value *string
		var pk int

		for pkRows.Next() {
			err = pkRows.Scan(&cid, &name, &ttype, &notnull, &dflt_value, &pk)
			if err != nil {
				return nil, err
			}

			// If pk column value is 1, then it's a primary key
			if pk == 1 {
				pksForTable = append(pksForTable, name)
			}
		}
		pkRows.Close()
	}
	return pks, nil
}

func (db *SQLiteDB) Query(ctx context.Context, sql string) (*sql.Rows, error) {
	return db.db.QueryContext(ctx, sql)
}

func (db *SQLiteDB) Close() error {
	return db.db.Close()
}
