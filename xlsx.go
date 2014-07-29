package main

import (
	"database/sql"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/cgi"
	"os"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/psmithuk/xlsx"
)

func TableNames(db *sql.DB) ([]string, error) {
	rows, err := db.Query("SELECT name FROM sqlite_master")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tableNames []string

	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, err
		}
		tableNames = append(tableNames, tableName)
	}

	return tableNames, err
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
		case time.Time:
			r.Cells[i] = xlsx.Cell{
				Type:  xlsx.CellTypeDatetime,
				Value: v.Format(time.RFC3339),
			}
		case string:
			r.Cells[i] = xlsx.Cell{
				Type:  xlsx.CellTypeInlineString,
				Value: fmt.Sprintf("%s", v),
			}
		case uint8, uint16, uint32, uint64, int8, int16, int32, int64, float32, float64:
			r.Cells[i] = xlsx.Cell{
				Type:  xlsx.CellTypeNumber,
				Value: fmt.Sprintf("%v", v),
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

func WriteXLSX(db *sql.DB, w io.Writer, tableName string) error {
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
	sh.Title = tableName
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

func contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

func Handler(w http.ResponseWriter, r *http.Request) {
	requestedTable := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]

	db, err := sql.Open("sqlite3", "/home/scraperwiki.sqlite")
	if err != nil {
		log.Fatal("db, err :=", db, err)
	}

	tableNames, err := TableNames(db)
	if err != nil {
		fmt.Printf("%s\n", err)
	}

	log.Printf("%s", tableNames)

	var tableName string
	if requestedTable == "alltables" {
		// TODO: Create XLSX with a sheet for each table
		tableName = tableNames[0]
	} else {
		if contains(tableNames, requestedTable) {
			tableName = requestedTable
		} else {
			log.Printf("Table %s does not exist", tableName)
		}

	}

	w.Header().Set("Content-Disposition", "attachment; filename="+tableName+".xlsx")
	w.Header().Set("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
	w.WriteHeader(http.StatusOK)
	err = WriteXLSX(db, w, tableName)
	if err != nil {
		log.Fatalf("%v", err)
	}
}

func main() {
	f, err := os.OpenFile("/tmp/cgi.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	defer f.Close()
	log.SetOutput(f)

	err = cgi.Serve(http.HandlerFunc(Handler))

	if err != nil {
		log.Fatalf("%v", err)
	}
}
