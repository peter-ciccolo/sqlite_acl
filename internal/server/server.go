package server

import (
	"context"

	"chroma1/internal/acl"
	"chroma1/internal/db"
	"chroma1/model/permissions"
)

type Server struct {
	aclManager *acl.ACLManager
	db         db.DB
}

func NewServer(ctx context.Context, aclStorage acl.ACLStorage, database db.DB) (*Server, error) {
	pks, err := database.GetPKs()
	if err != nil {
		return nil, err
	}
	man, err := acl.NewACLManager(ctx, aclStorage, pks)
	if err != nil {
		return nil, err
	}
	return &Server{
		aclManager: man,
		db:         database,
	}, nil
}

func (s *Server) Query(ctx context.Context, key string, req *QueryRequest) (*QueryResponse, error) {
	if err := s.aclManager.CheckPermissions(ctx, key, req.SQL); err != nil {
		return nil, err
	}
	rows, err := s.db.Query(req.SQL)
	if err != nil {
		return nil, err
	}
	res := &QueryResponse{
		Rows: make([]map[string]interface{}, 0),
	}
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		vals := make([]interface{}, len(cols))
		if err := rows.Scan(vals...); err != nil {
			return nil, err
		}
		valMap := make(map[string]interface{})
		for i, col := range cols {
			valMap[col] = vals[i]
		}
		res.Rows = append(res.Rows, valMap)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return res, nil
}

func (s *Server) GetPermissions(ctx context.Context, key string, req *AddPermissionsRequest) (*GetPermissionsResponse, error) {
	p, err := s.aclManager.GetPermissions(ctx, key, req.User)
	if err != nil {
		return nil, err
	}
	return &GetPermissionsResponse{
		Permissions: p,
	}, nil
}

func (s *Server) GetAllPermissions(ctx context.Context, key string) (*GetAllPermissionsResponse, error) {
	p, err := s.aclManager.GetAllPermissions(key)
	if err != nil {
		return nil, err
	}
	return &GetAllPermissionsResponse{
		Permissions: p,
	}, nil
}

func (s *Server) AddPermissions(ctx context.Context, key string, req *AddPermissionsRequest) error {
	return s.aclManager.AddPermissions(ctx, key, req.User, req.Permissions)
}

func (s *Server) RemovePermissions(ctx context.Context, key string, req *RemovePermissionsRequest) error {
	return s.aclManager.RemovePermissions(ctx, key, req.User, req.Permissions)
}

type QueryRequest struct {
	Key string `json:"key"`
	SQL string `json:"sql"`
}

type QueryResponse struct {
	Rows []map[string]interface{} `json:"rows"`
}

type GetPermissionsRequest struct {
	User string `json:"user"`
}

type GetPermissionsResponse struct {
	Permissions []*permissions.Permission `json:"permissions"`
}

type GetAllPermissionsResponse struct {
	Permissions map[string][]*permissions.Permission `json:"permissions"`
}

type AddPermissionsRequest struct {
	User        string                    `json:"user"`
	Permissions []*permissions.Permission `json:"permissions"`
}

type RemovePermissionsRequest struct {
	User        string                    `json:"user"`
	Permissions []*permissions.Permission `json:"permissions"`
}
