package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	_ "github.com/mattn/go-sqlite3"

	"chroma1/model/permissions"
)

const (
	aclTable = "SQLITE_ACLS"
)

type SQLiteACLStorage struct {
	db *sql.DB
}

func NewSQLiteACLStorage(ctx context.Context, connectionStr string) (*SQLiteACLStorage, error) {
	backing, err := sql.Open("sqlite3", connectionStr)
	if err != nil {
		return nil, err
	}

	s := &SQLiteACLStorage{
		db: backing,
	}

	if err := s.createTableIfMissing(ctx); err != nil {
		return nil, err
	}

	return s, nil
}

func (s *SQLiteACLStorage) StoreUserPerms(ctx context.Context, user string, perms []*permissions.Permission) error {
	b, err := json.Marshal(perms)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, fmt.Sprintf("UPDATE %s SET permissions_json = %s WHERE userid = %s", aclTable, string(b), user))
	return err
}

func (s *SQLiteACLStorage) GetUserPerms(ctx context.Context, user string) ([]*permissions.Permission, error) {
	row := s.db.QueryRowContext(ctx, fmt.Sprintf("SELECT permissions_json FROM %s WHERE userid = %s", aclTable, user))
	var jPerms string
	if err := row.Scan(&jPerms); err != nil {
		return nil, err
	}
	var perms []*permissions.Permission
	err := json.Unmarshal([]byte(jPerms), &perms)
	return perms, err
}

func (s *SQLiteACLStorage) GetAllUserInfo(ctx context.Context) (map[string][]*permissions.Permission, map[string]struct{}, map[string]string, error) {
	rows, err := s.db.QueryContext(ctx, fmt.Sprintf("SELECT userid, api_key, is_admin, permissions_json FROM %s", aclTable))
	defer rows.Close()
	if err != nil {
		return nil, nil, nil, err
	}
	userPerms := make(map[string][]*permissions.Permission)
	adminKeys := make(map[string]struct{})
	keyToUser := make(map[string]string)
	for rows.Next() {
		var userid string
		var key string
		var isAdmin int
		var jPerms string
		if err := rows.Scan(&userid, &key, &isAdmin, &jPerms); err != nil {
			return nil, nil, nil, err
		}
		keyToUser[key] = userid
		if isAdmin == 1 {
			adminKeys[key] = struct{}{}
		}

		var perms []*permissions.Permission
		err := json.Unmarshal([]byte(jPerms), &perms)
		if err != nil {
			return nil, nil, nil, err
		}
		userPerms[userid] = perms
	}
	if err := rows.Err(); err != nil {
		return nil, nil, nil, err
	}
	return userPerms, adminKeys, keyToUser, nil
}

func (s *SQLiteACLStorage) Close() error {
	return s.db.Close()
}

func (s *SQLiteACLStorage) createTableIfMissing(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (userid STRING PRIMARY KEY, api_key STRING, is_admin INTEGER, permissions_json STRING);"))
	return err
}
