package db

import (
	"context"
	"database/sql"
)

type DB interface {
	GetPKs(ctx context.Context) (map[string][]string, error)
	// Caller is responsible for calling Close() on the rows when done.
	Query(ctx context.Context, sql string) (*sql.Rows, error)
	Close() error
}
