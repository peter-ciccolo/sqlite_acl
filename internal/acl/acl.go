package acl

import (
	"context"
	"fmt"
	"strings"

	"chroma1/internal/parsing"
	"chroma1/model/permissions"

	"golang.org/x/exp/slices"
)

var (
	NotAdminError = fmt.Errorf("not an admin")
)

type ACLStorage interface {
	StoreUserPerms(ctx context.Context, user string, perms []*permissions.Permission) error
	GetUserPerms(ctx context.Context, user string) ([]*permissions.Permission, error)
	// Gets a map from user id to permissions, a set of keys belonging to admins, map from key to userid
	GetAllUserInfo(ctx context.Context) (map[string][]*permissions.Permission, map[string]struct{}, map[string]string, error)
	Close() error
}

type ACLManager struct {
	storage   ACLStorage                           // permanent storage for ACLs
	perms     map[string][]*permissions.Permission // map from user/key to permissions. TODO: replace with cache for distributed case.
	adminKeys map[string]struct{}
	keyToUser map[string]string
	tablePKs  map[string][]string
}

func NewACLManager(ctx context.Context, storage ACLStorage, tablePKs map[string][]string) (*ACLManager, error) {
	p, admins, keyToUser, err := storage.GetAllUserInfo(ctx)
	if err != nil {
		return nil, err
	}
	return &ACLManager{
		storage:   storage,
		tablePKs:  tablePKs,
		perms:     p,
		adminKeys: admins,
		keyToUser: keyToUser,
	}, nil
}

type InsufficientPermissionsError struct {
	failingRequirements []*parsing.RequiredPermission
}

func (err InsufficientPermissionsError) Error() string {
	var b strings.Builder
	b.WriteString("Insufficient permissions for:")
	for _, fr := range err.failingRequirements {
		b.WriteString(fmt.Sprintf("\n%s", fr.DebugString()))
	}
	return b.String()
}

// CheckPermissions checks if the given sql can be run by the given user. Will raise an error if not allowed.
func (acl *ACLManager) CheckPermissions(ctx context.Context, key, sql string) error {
	user, ok := acl.keyToUser[key]
	if !ok {
		return fmt.Errorf("no such key found")
	}
	reqs, err := parsing.Parse(sql, acl.tablePKs)
	if err != nil {
		return err
	}

	perms, err := acl.getPerms(ctx, user)
	if err != nil {
		return err
	}

	failingReqs := make([]*parsing.RequiredPermission, 0)
	for _, req := range reqs {
		if !reqPasses(req.Perm, perms) {
			failingReqs = append(failingReqs, req)
		}
	}
	if len(failingReqs) > 0 {
		return InsufficientPermissionsError{
			failingRequirements: failingReqs,
		}
	}
	return nil
}

func (acl *ACLManager) AddPermissions(ctx context.Context, key, user string, toAdd []*permissions.Permission) error {
	if _, ok := acl.adminKeys[key]; !ok {
		return NotAdminError
	}

	perms, err := acl.getPerms(ctx, user)
	if err != nil {
		return err
	}

	for _, ta := range toAdd {
		for _, p := range perms {
			if p.Table == ta.Table && p.Type == ta.Type {
				updatePermAdd(p, ta)
			}
		}
		perms = append(perms, ta)
	}

	if err := acl.storage.StoreUserPerms(ctx, user, perms); err != nil {
		return fmt.Errorf("error storing user permissions for %s: %w", user, err)
	}

	return nil
}

func (acl *ACLManager) RemovePermissions(ctx context.Context, key, user string, toRem []*permissions.Permission) error {
	if _, ok := acl.adminKeys[key]; !ok {
		return NotAdminError
	}

	perms, err := acl.getPerms(ctx, user)
	if err != nil {
		return err
	}

	for _, ta := range toRem {
		for i, p := range perms {
			if p.Table == ta.Table && p.Type == ta.Type {
				shouldDelete, err := updatePermRemove(p, ta)
				if err != nil {
					return err
				}
				if shouldDelete {
					perms[i] = perms[len(perms)-1]
					perms = perms[:len(perms)-1]
					break
				}
			}
		}
	}

	if err := acl.storage.StoreUserPerms(ctx, user, perms); err != nil {
		return fmt.Errorf("error storing user permissions for %s: %w", user, err)
	}

	return nil
}

func (acl *ACLManager) GetPermissions(ctx context.Context, key, user string) ([]*permissions.Permission, error) {
	if _, ok := acl.adminKeys[key]; !ok {
		return nil, NotAdminError
	}

	return acl.getPerms(ctx, user)
}

func (acl *ACLManager) GetAllPermissions(key string) (map[string][]*permissions.Permission, error) {
	if _, ok := acl.adminKeys[key]; !ok {
		return nil, NotAdminError
	}

	// TODO: use the storage.GetAllPermissions method in the distributed case
	return acl.perms, nil
}

func reqPasses(req permissions.Permission, perms []*permissions.Permission) bool {
	for _, p := range perms {
		if p.Table == req.Table && p.Type == req.Type {
			if p.RowKeys == nil {
				return true // blanket permissions
			}
			if req.RowKeys == nil {
				return false // requires full-table, but only a subset is allowed
			}
			for _, k := range req.RowKeys {
				if _, ok := slices.BinarySearchFunc(p.RowKeys, k, pkCmp); !ok {
					return false
				}
			}
		}
	}
	return false // no relevant permission found
}

func updatePermAdd(original, addition *permissions.Permission) {
	if addition.RowKeys == nil {
		original.RowKeys = nil
	}
	if original.RowKeys == nil {
		return
	}
	// TODO: optimize this (via merge) for large cases
	for _, k := range addition.RowKeys {
		if idx, ok := slices.BinarySearchFunc(original.RowKeys, k, pkCmp); !ok {
			original.RowKeys = append(original.RowKeys, []string{}) // append a dummy value to expand the slice by 1
			copy(original.RowKeys[idx+1:], original.RowKeys[idx:])
			original.RowKeys[idx] = k
		}
	}
}

// Returns a bool indicating whether the targeted permission is now empty and can thus be deleted.
// Returns an error if trying to remove specific rows from a blanket permission.
func updatePermRemove(original, toRemove *permissions.Permission) (bool, error) {
	if toRemove.RowKeys != nil && original.RowKeys == nil {
		return false, fmt.Errorf("cannot remove specific keys from blanket permission")
	}
	if toRemove.RowKeys == nil && original.RowKeys == nil {
		return true, nil
	}
	if toRemove.RowKeys == nil && original.RowKeys != nil {
		return false, nil // removing blanket permissions from a subset permission is a no-op
	}
	for _, k := range toRemove.RowKeys {
		if idx, ok := slices.BinarySearchFunc(original.RowKeys, k, pkCmp); ok {
			original.RowKeys = append(original.RowKeys[:idx], original.RowKeys[idx+1:]...)
		}
	}
	return len(original.RowKeys) == 0, nil
}

func (acl *ACLManager) getPerms(ctx context.Context, user string) ([]*permissions.Permission, error) {
	perms, ok := acl.perms[user]
	var err error
	if !ok {
		perms, err = acl.storage.GetUserPerms(ctx, user)
		if err != nil {
			return nil, fmt.Errorf("error fetching user %s from storage: %w", user, err)
		}
	}
	acl.perms[user] = perms
	return perms, nil
}

func pkCmp(a, b []string) int {
	for i := range a {
		cmpVal := strings.Compare(a[i], b[i])
		if cmpVal != 0 {
			return cmpVal
		}
	}
	return 0
}
