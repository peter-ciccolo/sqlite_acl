package permissions

import (
	"fmt"
)

type PermissionType int

const (
	Read = iota + 1
	Write
)

func (pt PermissionType) String() string {
	switch pt {
	case Read:
		return "READ"
	case Write:
		return "WRITE"
	}
	panic(fmt.Sprintf("unknown permission type %v", int(pt)))
}

// Permission represents permissions on a given table or table subset.
type Permission struct {
	Type PermissionType
	// name of the table containing the rows
	Table string
	// a list of the allowed rows (by primary key). Empty represents 'all rows'.
	// Keys are stored as lists of strings, with each item being one column. Ordering matches the PK definition.
	// Kept in sorted order.
	RowKeys [][]string
}
