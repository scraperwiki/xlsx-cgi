w
package main

import (
	"database/sql"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/cgi"
	"strconv"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/psmithuk/xlsx"
)

func TableName(db *sql.DB) (string, error) {
	row := db.QueryRow("SELECT name FROM sqlite_master")
	var tableName string
	err := row.Scan(&tableName)
	return tableName, err
}

func RowCount(db *sql.DB, tablename string) (int, error) {
	row := db.QueryRow("SELECT COUNT(*) FROM " + tablename)
	var rowCount int
	err := row.Scan(&rowCount)
	return rowCount, err
}

func ColumnTypes(db *sql.DB, tablename string) ([]xlsx.Column, []interface{}, []interface{}, error) {
	rows, err := db.Query(fmt.Sprintf("SELECT * FROM %s limit 1", tablename))
	if err != nil {
		return nil, nil, nil, err
	}

	cols, err := rows.Columns()
	if err != nil {
		return nil, nil, nil, err
	}

	values := make([]interface{}, len(cols))
	scanArgs := make([]interface{}, len(cols))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	rows.Next()
	err = rows.Scan(scanArgs...)
	if err != nil {
		return nil, nil, nil, err
	}

	var c []xlsx.Column

	for _, colName := range cols {
		c = append(c, xlsx.Column{Name: colName, Width: 10})
	}

	return c, values, scanArgs, nil
}

func PopulateRow(r xlsx.Row, values []interface{}) error {
	for i, v := range values {
		switch v := v.(type) {
		case nil:
			r.Cells[i] = xlsx.Cell{
				Type:  xlsx.CellTypeInlineString,
				Value: "",
			}
		case uint64:
			r.Cells[i] = xlsx.Cell{
				Type:  xlsx.CellTypeNumber,
				Value: strconv.FormatUint(v, 10),
			}
		case int64:
			r.Cells[i] = xlsx.Cell{
				Type:  xlsx.CellTypeNumber,
				Value: strconv.FormatInt(v, 10),
			}
		case time.Time:
			r.Cells[i] = xlsx.Cell{
				Type:  xlsx.CellTypeDatetime,
				Value: v.Format(time.RFC3339),
			}
		default:
			r.Cells[i] = xlsx.Cell{
				Type:  xlsx.CellTypeInlineString,
				Value: fmt.Sprintf("%s", v),
			}
		}
	}
	return nil
}

func WriteXLSX(db *sql.DB, w io.Writer) error {
	tableName, err := TableName(db)
	if err != nil {
		fmt.Printf("%s\n", err)
	}

	rowCount, err := RowCount(db, tableName)
	if err != nil {
		panic(err)
	}

	cols, values, scanArgs, err := ColumnTypes(db, tableName)
	if err != nil {
		panic(err)
	}

	ww := xlsx.NewWorkbookWriter(w)

	sh := xlsx.NewSheetWithColumns(cols)
	sw, err := ww.NewSheetWriter(&sh)

	rows, err := db.Query("SELECT * FROM " + tableName)
	if err != nil {
		log.Fatal(err)
	}

	for i := 0; i < rowCount; i++ {
		rows.Next()
		err = rows.Scan(scanArgs...)
		if err != nil {
			panic(err)
		}

		r := sh.NewRow()
		err = PopulateRow(r, values)
		if err != nil {
			panic(err)
		}

		err = sw.WriteRows([]xlsx.Row{r})
		if err != nil {
			panic(err)
		}
	}

	err = rows.Close()
	if err != nil {
		panic(err)
	}

	err = ww.Close()
	if err != nil {
		panic(err)
	}
	return err
}

func Handler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
	w.WriteHeader(http.StatusOK)

	db, err := sql.Open("sqlite3", "../scraperwiki.sqlite")
	if err != nil {
		log.Fatal("db, err :=", db, err)
	}

	WriteXLSX(db, w)
}

func main() {
	cgi.Serve(http.HandlerFunc(Handler))
}
