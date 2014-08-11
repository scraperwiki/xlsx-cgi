package main

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"net/http/cgi"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/psmithuk/xlsx"
)

var (
	tableNameCheck = regexp.MustCompile(`^[0-9a-zA-Z_]+$`)
	pageNumParse   = regexp.MustCompile(`page_([0-9]+)`)
	pathParse      = regexp.MustCompile(`\/[a-z0-9]+\/[a-z0-9]+\/cgi-bin\/[^/]+(?:\/([0-9a-zA-Z_]+)\/?|\/?)$`)
)

func TableNames(db *sql.DB) ([]string, error) {
	rows, err := db.Query(`SELECT tbl_name FROM sqlite_master WHERE type="table"`)
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

func ColumnDefinions(db *sql.DB, tablename string) (colNames, colTypes []string, err error) {
	rows, err := db.Query(fmt.Sprintf("SELECT * FROM [%s] limit 1", tablename))
	if err != nil {
		return nil, nil, err
	}

	colNames, err = rows.Columns()
	if err != nil {
		return nil, nil, err
	}

	rows, err = db.Query(fmt.Sprintf("PRAGMA table_info([%s])", tablename))

	for rows.Next() {
		var colType string
		unused := []byte{}
		u := &unused
		err := rows.Scan(u, u, &colType, u, u, u)
		if err != nil {
			return nil, nil, err
		}
		colTypes = append(colTypes, colType)
	}

	return colNames, colTypes, err
}

func ColumnStorage(db *sql.DB, tablename string, nColumns int) ([]interface{}, []interface{}, error) {
	values := make([]interface{}, nColumns)
	scanArgs := make([]interface{}, nColumns)
	for i := range values {
		scanArgs[i] = &values[i]
	}

	return values, scanArgs, nil
}

func PopulateRow(r xlsx.Row, values []interface{}) error {
	for i, v := range values {
		switch v := v.(type) {
		case nil:
			r.Cells[i] = xlsx.Cell{
				Type:  xlsx.CellTypeInlineString,
				Value: "",
			}
		case Date:
			r.Cells[i] = xlsx.Cell{
				Type:  xlsx.CellTypeDate,
				Value: v.Format(time.RFC3339),
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
		case bool:
			r.Cells[i] = xlsx.Cell{
				Type:  xlsx.CellTypeInlineString,
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

type Date struct{ time.Time }

func WriteSheet(ww *xlsx.WorkbookWriter, db *sql.DB, tablename string) error {

	colNames, colTypes, err := ColumnDefinions(db, tablename)
	if err != nil {
		return fmt.Errorf("failed to get column definitions: %q", err)
	}

	nColumns := len(colNames)
	storage, storagePtrs, err := ColumnStorage(db, tablename, nColumns)
	if err != nil {
		return err
	}

	var xlsxCols []xlsx.Column
	header := xlsx.Row{[]xlsx.Cell{}}

	for _, colName := range colNames {
		header.Cells = append(header.Cells, xlsx.Cell{
			Type:  xlsx.CellTypeInlineString,
			Value: colName,
		})
		xlsxCols = append(xlsxCols, xlsx.Column{Name: colName, Width: 10})
	}

	sh := xlsx.NewSheetWithColumns(xlsxCols)
	sh.Title = tablename
	sw, err := ww.NewSheetWriter(&sh)
	if err != nil {
		return err
	}

	err = sw.WriteRows([]xlsx.Row{header})
	if err != nil {
		return err
	}

	rows, err := db.Query(fmt.Sprintf("SELECT * FROM [%s]", tablename))
	if err != nil {
		return err
	}

	scan := func() ([]interface{}, error) {
		err = rows.Scan(storagePtrs...)
		if err != nil {
			return nil, nil
		}

		// Re-cast 'date' values as the 'Date' type so that we can pass them
		// to Excel in the right form.
		var result []interface{}
		for i, v := range storage {
			switch colTypes[i] {
			case "date":
				v = Date{v.(time.Time)}
			}
			result = append(result, v)
		}
		return result, nil
	}

	for rows.Next() {
		values, err := scan()
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

	var devTables bool

	if contains(r.URL.Query()["devTables"], "true") {
		devTables = true
	}

	if !tableNameCheck.MatchString(requestedTable) && requestedTable != "" {
		http.Error(w, fmt.Sprintf("Invalid table name: %s", requestedTable), http.StatusNotFound)
		return
	}

	db, err := sql.Open("sqlite3", os.ExpandEnv("$HOME/scraperwiki.sqlite"))
	if err != nil {
		log.Print(err)
		http.Error(w, "500: Problem opening database.", http.StatusInternalServerError)
		return
	}

	tableNames, err := TableNames(db)
	if err != nil {
		log.Print(err)
		http.Error(w, "500: Could not retrieve table names.", http.StatusInternalServerError)
		return
	}

	var tablesToWrite []string
	gridsToWrite := []struct{ URL, Title string }{}

	if requestedTable == "" {
		requestedTable = "all_tables"
		tablesToWrite = tableNames
		if contains(tableNames, "_grids") {
			gridsToWrite, err = AllGrids(db)
			if err != nil {
				log.Print(err)
				http.Error(w, "500: Could not get grids.", http.StatusInternalServerError)
				return
			}
		}
	} else if contains(tableNames, "_grids") && pageNumParse.MatchString(requestedTable) {
		pageNumString := pageNumParse.ReplaceAllString(requestedTable, "$1")
		pageNum, err := strconv.Atoi(pageNumString)
		if err != nil {
			http.Error(w, fmt.Sprintf("404: Invalid page number.", pageNumString), http.StatusNotFound)
			return
		}

		gridURL, err := GridURL(db, pageNum)
		switch {
		case err == sql.ErrNoRows:
			http.Error(w, fmt.Sprintf("404: Page %v does not exist.", pageNum), http.StatusNotFound)
		case err != nil:
			log.Printf("Error in GridURL: %v", err)
			http.Error(w, "", http.StatusInternalServerError)
			return
		default:
			gridTitle, err := GridTitle(db, pageNum)
			if err != nil {
				log.Print(err)
				http.Error(w, fmt.Sprintf("500: Could not get grid title for page %v", pageNum), http.StatusInternalServerError)
				return
			}
			gridsToWrite = append(gridsToWrite, struct{ URL, Title string }{gridURL, gridTitle})
		}
	} else {

		if contains(tableNames, requestedTable) {
			tablesToWrite = append(tablesToWrite, requestedTable)
		} else {
			http.Error(w, fmt.Sprintf("404: Table %s does not exist.", requestedTable), http.StatusNotFound)
			return
		}
	}

	if len(tablesToWrite) == 0 && len(gridsToWrite) == 0 {
		http.Error(w, "404: No tables found.", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Disposition", "attachment; filename="+requestedTable+".xlsx")
	w.Header().Set("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
	w.WriteHeader(http.StatusOK)

	ww := xlsx.NewWorkbookWriter(w)
	defer ww.Close()

	for _, tableName := range tablesToWrite {
		if !strings.HasPrefix(tableName, "_") || devTables {
			err = WriteSheet(ww, db, tableName)
			if err != nil {
				log.Print(err)
				http.Error(w, "500: Could write sheet", http.StatusInternalServerError)
				return
			}
		}
	}

	tables := make(chan HTMLTable)
	go func() {
		defer close(tables)
		for _, grid := range gridsToWrite {
			ParseHTML(grid.URL, tables, grid.Title)
		}
	}()
	for table := range tables {
		err = WriteGridSheet(ww, table)
		if err != nil {
			log.Print(err)
			http.Error(w, "500: Could not write grid", http.StatusInternalServerError)
			return
		}
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
