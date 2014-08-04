package main

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"net/http/cgi"
	"os"
	"regexp"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/psmithuk/xlsx"
)

var (
	tableNameCheck = regexp.MustCompile(`^[0-9a-z_]+$`)
	pageNumParse   = regexp.MustCompile(`\/[a-z0-9]+\/[a-z0-9]+\/cgi-bin\/xlsx\/page_([0-9]+)`)
	pathParse      = regexp.MustCompile(`\/[a-z0-9]+\/[a-z0-9]+\/cgi-bin\/xlsx\/?([0-9a-z_]*)\/?`)
	gridPathParse  = regexp.MustCompile(`.*(\/http\/grids\/[a-z0-9]+\.html)`)
)

func GridURL(db *sql.DB, pageNum int) (string, error) {
	row := db.QueryRow("SELECT url FROM _grids where number=?", pageNum)
	var gridURL string
	err := row.Scan(&gridURL)
	return gridURL, err
}

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
	row := db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM [%s]", tablename))
	var rowCount int
	err := row.Scan(&rowCount)
	return rowCount, err
}

func ColumnTypes(db *sql.DB, tablename string) ([]xlsx.Column, []interface{}, []interface{}, error) {
	rows, err := db.Query(fmt.Sprintf("SELECT * FROM [%s] limit 1", tablename))
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

func WriteSheet(ww *xlsx.WorkbookWriter, db *sql.DB, tableName string) error {
	rowCount, err := RowCount(db, tableName)
	if err != nil {
		return err
	}

	cols, values, scanArgs, err := ColumnTypes(db, tableName)
	if err != nil {
		return err
	}

	sh := xlsx.NewSheetWithColumns(cols)
	sh.Title = tableName
	sw, err := ww.NewSheetWriter(&sh)
	if err != nil {
		return err
	}

	rows, err := db.Query(fmt.Sprintf("SELECT * FROM [%s]", tableName))
	if err != nil {
		return err
	}

	for i := 0; i < rowCount; i++ {
		rows.Next()
		err = rows.Scan(scanArgs...)
		if err != nil {
			return err
		}

		r := sh.NewRow()
		err = PopulateRow(r, values)
		if err != nil {
			return err
		}

		err = sw.WriteRows([]xlsx.Row{r})
		if err != nil {
			return err
		}
	}

	err = rows.Close()
	if err != nil {
		return err
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
	requestedTable := pathParse.ReplaceAllString(r.URL.Path, "$1")

	if !tableNameCheck.MatchString(requestedTable) && requestedTable != "" {
		panic(fmt.Sprintf("Invalid table name: %s", requestedTable))
	}

	db, err := sql.Open("sqlite3", os.ExpandEnv("$HOME/scraperwiki.sqlite"))
	if err != nil {
		panic(err)
	}

	tableNames, err := TableNames(db)
	if err != nil {
		panic(err)
	}

	var tablesToWrite []string
	var gridsToWrite []string

	if contains(tableNames, "_grids") {
		// TODO: handle all grids at once case
		pageNum, err := strconv.Atoi(pageNumParse.ReplaceAllString(r.URL.Path, "$1"))
		if err != nil {
			panic(err)
		}

		gridURL, err := GridURL(db, pageNum)
		switch {
		case err == sql.ErrNoRows:
			panic(fmt.Sprintf("Page %v does not exist", pageNum))
		case err != nil:
			panic(err)
		default:
			gridsToWrite = append(gridsToWrite, gridURL)
		}
	} else {
		if requestedTable == "" {
			tablesToWrite = tableNames
			requestedTable = "all_tables"
		} else {
			if contains(tableNames, requestedTable) {
				tablesToWrite = append(tablesToWrite, requestedTable)
			} else {
				panic(fmt.Sprintf("Table %s does not exist", requestedTable))
			}
		}
	}

	w.Header().Set("Content-Disposition", "attachment; filename="+requestedTable+".xlsx")
	w.Header().Set("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
	w.WriteHeader(http.StatusOK)

	ww := xlsx.NewWorkbookWriter(w)

	for _, tableName := range tablesToWrite {
		err = WriteSheet(ww, db, tableName)
		if err != nil {
			panic(err)
		}
	}

	for _, gridURL := range gridsToWrite {
		err = WriteGridSheet(ww, db, gridURL)
		if err != nil {
			panic(err)
		}
	}

	err = ww.Close()

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
