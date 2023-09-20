package parsing_test

import (
	"fmt"
	"testing"

	"github.com/kr/pretty"
	"vitess.io/vitess/go/vt/sqlparser"
)

func sqlAST(sql string) {
	ast, _, err := sqlparser.Parse2(sql)
	if err != nil {
		fmt.Printf("\n%v", err)
	}
	fmt.Println(pretty.Sprint(ast))
}

func TestAST(t *testing.T) {
	sqlAST("SELECT * FROM T1, T2")
	//sqlAST("SELECT T.x, R.y FROM T LEFT JOIN R ON T.z = R.w WHERE T.a = 2")
	//sqlAST("DELETE FROM table2 WHERE (k1 = 5 AND k2 = 10) OR (k1 = 6 AND k2 = 11)")
	t.Fatal()
}
