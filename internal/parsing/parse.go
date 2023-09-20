package parsing

import (
	"fmt"
	"strings"

	"chroma1/model/permissions"

	"vitess.io/vitess/go/vt/sqlparser"
)

type RequiredPermission struct {
	Perm     permissions.Permission
	fromNode sqlparser.SQLNode
}

func (rp *RequiredPermission) DebugString() string {
	var ds strings.Builder
	ds.WriteString(fmt.Sprintf("Table %s: (%sv) for ", rp.Perm.Table, rp.Perm.Type))
	if rp.Perm.RowKeys == nil {
		ds.WriteString("all")
	} else {
		ds.WriteString(fmt.Sprintf("%d", len(rp.Perm.RowKeys)))
	}
	ds.WriteString(" keys (due to \"")
	ds.WriteString(sqlparser.String(rp.fromNode))
	ds.WriteString(")")
	return ds.String()
}

// Parse a sql statement, return the list of required permissions.
func Parse(sql string, tableToPK map[string][]string) ([]*RequiredPermission, error) {
	pieces, err := sqlparser.SplitStatementToPieces(sql)
	if err != nil {
		return nil, err
	}

	reqs := make([]*RequiredPermission, 0)

	for _, p := range pieces {
		stmt, _, err := sqlparser.Parse2(p)
		if err != nil {
			return nil, err
		}

		err = sqlparser.Walk(func(node sqlparser.SQLNode) (cont bool, err error) {
			switch v := node.(type) {
			case *sqlparser.Select:
				// TODO: handle joins
				tableName := sqlparser.String(v.From[0])
				reqs = append(reqs, &RequiredPermission{
					fromNode: node,
					Perm: permissions.Permission{
						Table:   tableName,
						Type:    permissions.Read,
						RowKeys: whereNodeToRows(v.Where, tableToPK[tableName]),
					},
				})
			case *sqlparser.Update:
				if len(v.TableExprs) > 1 {
					// SQLite does not support this.
					return false, fmt.Errorf("unsupported: found multiple table updates from '%s'", sqlparser.String(v))
				}
				tableName := sqlparser.String(v.TableExprs[0])

				reqs = append(reqs, &RequiredPermission{
					fromNode: node,
					Perm: permissions.Permission{
						Table:   tableName,
						Type:    permissions.Write,
						RowKeys: whereNodeToRows(v.Where, tableToPK[tableName]),
					},
				})
			case *sqlparser.Delete:
				if len(v.TableExprs) > 1 {
					// SQLite does not support this.
					return false, fmt.Errorf("unsupported: found multiple table deletions from '%s'", sqlparser.String(v))
				}
				tableName := sqlparser.String(v.TableExprs[0])

				reqs = append(reqs, &RequiredPermission{
					fromNode: node,
					Perm: permissions.Permission{
						Table:   tableName,
						Type:    permissions.Write,
						RowKeys: whereNodeToRows(v.Where, tableToPK[tableName]),
					},
				})
			case *sqlparser.Insert:
				reqs = append(reqs, &RequiredPermission{
					fromNode: node,
					Perm: permissions.Permission{
						Table: sqlparser.String(v.Table.Expr),
						Type:  permissions.Write,
					},
				})
			}
			return true, nil
		}, stmt)
		if err != nil {
			return nil, err
		}
	}

	return reqs, nil
}

// Helper to get the directly specified rows given a WHERE clause. Each entry is a map of column requirements.
// e.g. {"x": "5", "y":"foo"} means the row with x = 5 and y = "foo".
// If empty, the rows were not directly specified.
func whereNodeToRows(where *sqlparser.Where, pk []string) [][]string {
	if where == nil {
		return nil
	}
	rows := make([][]string, 0)
	for _, spec := range recurseOnWhereExpr(where.Expr) {
		row := make([]string, len(pk))
		for i, c := range pk {
			if v, ok := spec[c]; ok {
				row[i] = v
			} else {
				return nil // if any row returned did not specify the primary key, then the whole expression needs full permissions.
			}
		}
		rows = append(rows, row)
	}
	if len(rows) == 0 {
		return nil
	}
	return rows
}

func recurseOnWhereExpr(expr sqlparser.Expr) []map[string]string {
	switch v := expr.(type) {
	case *sqlparser.ComparisonExpr:
		if v.Operator != sqlparser.EqualOp {
			return nil // if not equality, could be anything.
		}
		if col, ok := v.Left.(*sqlparser.ColName); ok {
			if val, ok := v.Right.(*sqlparser.Literal); ok {
				return []map[string]string{{col.Name.String(): val.Val}}
			}
		}
		return nil // left side was not a column or right was not a value
	case *sqlparser.AndExpr:
		lV := recurseOnWhereExpr(v.Left)
		rV := recurseOnWhereExpr(v.Right)
		// TODO: handle cross products
		if len(lV) != 1 || len(rV) != 1 {
			return nil
		}
		specced := make(map[string]string)
		for k, v := range lV[0] {
			specced[k] = v
		}
		for k, v := range rV[0] {
			specced[k] = v
		}
		return []map[string]string{specced}
	case *sqlparser.OrExpr:
		specced := make([]map[string]string, 0)
		lV := recurseOnWhereExpr(v.Left)
		rV := recurseOnWhereExpr(v.Right)
		if lV == nil || rV == nil {
			return nil
		}
		specced = append(specced, lV...)
		specced = append(specced, rV...)
		return specced
	}

	return nil
}
