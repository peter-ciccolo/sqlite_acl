package parsing_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"chroma1/internal/parsing"
	"chroma1/model/permissions"
)

func TestInsert(t *testing.T) {
	testcases := []struct {
		sql string
		exp permissions.Permission
	}{
		{
			sql: "INSERT INTO example (id, name) VALUES (1, 'test')",
			exp: permissions.Permission{
				Type:  permissions.Write,
				Table: "example",
			},
		},
		{
			sql: "INSERT INTO example VALUES (1, 'test')",
			exp: permissions.Permission{
				Type:  permissions.Write,
				Table: "example",
			},
		},
	}
	for i, tc := range testcases {
		t.Run(fmt.Sprintf("TestInsert case %v", i), func(t *testing.T) {
			reqs, err := parsing.Parse(tc.sql, nil)
			assert.NoError(t, err)
			assert.Len(t, reqs, 1)
			assert.Equal(t, tc.exp, reqs[0].Perm)
		})
	}
}

func TestDelete(t *testing.T) {
	t1 := "table1"
	t2 := "table2"
	pks := map[string][]string{
		t1: {"k"},
		t2: {"k1", "k2"},
	}

	testcases := []struct {
		sql string
		exp permissions.Permission
	}{
		{
			sql: "DELETE FROM table1",
			exp: permissions.Permission{
				Type:  permissions.Write,
				Table: t1,
			},
		},
		{
			sql: "DELETE FROM table1 WHERE x = 5",
			exp: permissions.Permission{
				Type:  permissions.Write,
				Table: t1,
			},
		},
		{
			sql: "DELETE FROM table1 WHERE k = 5",
			exp: permissions.Permission{
				Type:    permissions.Write,
				Table:   t1,
				RowKeys: [][]string{{"5"}},
			},
		},
		{
			sql: "DELETE FROM table1 WHERE k = 5 OR k = 10",
			exp: permissions.Permission{
				Type:    permissions.Write,
				Table:   t1,
				RowKeys: [][]string{{"5"}, {"10"}},
			},
		},
		{
			sql: "DELETE FROM table1 WHERE k = 5 AND y = 10",
			exp: permissions.Permission{
				Type:    permissions.Write,
				Table:   t1,
				RowKeys: [][]string{{"5"}},
			},
		},
		{
			sql: "DELETE FROM table1 WHERE k = 5 OR y = 10",
			exp: permissions.Permission{
				Type:  permissions.Write,
				Table: t1,
			},
		},
		{
			sql: "DELETE FROM table1 WHERE k > 5",
			exp: permissions.Permission{
				Type:  permissions.Write,
				Table: t1,
			},
		},
		{
			sql: "DELETE FROM table1 WHERE (k = 5) OR (k = 6 AND y = 10)",
			exp: permissions.Permission{
				Type:    permissions.Write,
				Table:   t1,
				RowKeys: [][]string{{"5"}, {"6"}},
			},
		},
		{
			sql: "DELETE FROM table2 WHERE k1 = 5",
			exp: permissions.Permission{
				Type:  permissions.Write,
				Table: t2,
			},
		},
		{
			sql: "DELETE FROM table2 WHERE k1 = 5 AND k2 = 10",
			exp: permissions.Permission{
				Type:    permissions.Write,
				Table:   t2,
				RowKeys: [][]string{{"5", "10"}},
			},
		},
		{
			sql: "DELETE FROM table2 WHERE k1 = 5 AND k2 = 10",
			exp: permissions.Permission{
				Type:    permissions.Write,
				Table:   t2,
				RowKeys: [][]string{{"5", "10"}},
			},
		},
		{
			sql: "DELETE FROM table2 WHERE (k1 = 5 AND k2 = 10) OR (k1 = 6 AND k2 = 11)",
			exp: permissions.Permission{
				Type:    permissions.Write,
				Table:   t2,
				RowKeys: [][]string{{"5", "10"}, {"6", "11"}},
			},
		},
	}
	for i, tc := range testcases {
		t.Run(fmt.Sprintf("TestDelete case %v", i), func(t *testing.T) {
			reqs, err := parsing.Parse(tc.sql, pks)
			assert.NoError(t, err)
			assert.Len(t, reqs, 1)
			assert.Equal(t, tc.exp, reqs[0].Perm)
		})
	}
}

func TestUpdate(t *testing.T) {
	t1 := "table1"
	t2 := "table2"
	pks := map[string][]string{
		t1: {"k"},
		t2: {"k1", "k2"},
	}

	testcases := []struct {
		sql string
		exp permissions.Permission
	}{
		{
			sql: "UPDATE table1 SET x = 5",
			exp: permissions.Permission{
				Type:  permissions.Write,
				Table: t1,
			},
		},
		{
			sql: "UPDATE table1 SET x = 5 WHERE k = 10",
			exp: permissions.Permission{
				Type:    permissions.Write,
				Table:   t1,
				RowKeys: [][]string{{"10"}},
			},
		},
		{
			sql: "UPDATE table2 SET x = 5 WHERE k1 = 10 AND k2 = 11",
			exp: permissions.Permission{
				Type:    permissions.Write,
				Table:   t2,
				RowKeys: [][]string{{"10", "11"}},
			},
		},
	}
	for i, tc := range testcases {
		t.Run(fmt.Sprintf("TestUpdate case %v", i), func(t *testing.T) {
			reqs, err := parsing.Parse(tc.sql, pks)
			assert.NoError(t, err)
			assert.Len(t, reqs, 1)
			assert.Equal(t, tc.exp, reqs[0].Perm)
		})
	}
}

func TestSelect(t *testing.T) {
	t1 := "table1"
	t2 := "table2"
	pks := map[string][]string{
		t1: {"k"},
		t2: {"k1", "k2"},
	}

	testcases := []struct {
		sql string
		exp permissions.Permission
	}{
		{
			sql: "SELECT * FROM table1",
			exp: permissions.Permission{
				Type:  permissions.Read,
				Table: t1,
			},
		},
		{
			sql: "SELECT a, b FROM table1",
			exp: permissions.Permission{
				Type:  permissions.Read,
				Table: t1,
			},
		},
		{
			sql: "SELECT a, b FROM table1 WHERE a = 1",
			exp: permissions.Permission{
				Type:  permissions.Read,
				Table: t1,
			},
		},
		{
			sql: "SELECT a, b FROM table1 WHERE k = 10",
			exp: permissions.Permission{
				Type:    permissions.Read,
				Table:   t1,
				RowKeys: [][]string{{"10"}},
			},
		},
		{
			sql: "SELECT a, b FROM table2 WHERE k1 = 10",
			exp: permissions.Permission{
				Type:  permissions.Read,
				Table: t2,
			},
		},
		{
			sql: "SELECT a, b FROM table2 WHERE k1 = 10 AND k2 = 11",
			exp: permissions.Permission{
				Type:    permissions.Read,
				Table:   t2,
				RowKeys: [][]string{{"10", "11"}},
			},
		},
	}
	for i, tc := range testcases {
		t.Run(fmt.Sprintf("TestSelect case %v", i), func(t *testing.T) {
			reqs, err := parsing.Parse(tc.sql, pks)
			assert.NoError(t, err)
			assert.Len(t, reqs, 1)
			assert.Equal(t, tc.exp, reqs[0].Perm)
		})
	}
}

func TestSelectFailing(t *testing.T) {
	t1 := "table1"
	t2 := "table2"
	pks := map[string][]string{
		t1: {"k"},
		t2: {"k1", "k2"},
	}

	testcases := []struct {
		sql string
		exp []permissions.Permission
	}{
		{
			sql: "SELECT * FROM table1, table2",
			exp: []permissions.Permission{
				{
					Type:  permissions.Read,
					Table: t1,
				},
				{
					Type:  permissions.Read,
					Table: t2,
				},
			},
		},
		{
			sql: "SELECT table1.x, table2.y FROM table1 INNER JOIN table2",
			exp: []permissions.Permission{
				{
					Type:  permissions.Read,
					Table: t1,
				},
				{
					Type:  permissions.Read,
					Table: t2,
				},
			},
		},

	}
	for i, tc := range testcases {
		t.Run(fmt.Sprintf("TestSelectFailing case %v", i), func(t *testing.T) {
			reqs, err := parsing.Parse(tc.sql, pks)
			assert.NoError(t, err)
			assert.Len(t, reqs, len(tc.exp))
			for i := range reqs {
				assert.Equal(t, tc.exp[i], reqs[i].Perm)
			}
		})
	}
}
