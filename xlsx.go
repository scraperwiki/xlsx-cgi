package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"runtime/pprof"
	"strconv"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/psmithuk/xlsx"
)

var memprofile = flag.String("memprofile", "", "write memory profile to this file")
var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")

func RowCount(db *sql.DB, tablename string) (int, error) {
	row := db.QueryRow("SELECT COUNT(*) FROM tablename=?", tablename)
	var rowCount int
	err := row.Scan(&rowCount)
	if err != nil {
		return 0, err
	}
	return rowCount, nil
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

func main() {
	flag.Parse()

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	db, err := sql.Open("sqlite3", "scraperwiki.sqlite")
	if err != nil {
		log.Fatal("db, err :=", db, err)
	}

	rowCount, err := RowCount(db, "tweets")
	if err != nil {
		panic(err)
	}

	cols, values, scanArgs, err := ColumnTypes(db, "tweets")
	if err != nil {
		panic(err)
	}

	outputfile, err := os.Create("test.xlsx")
	ww := xlsx.NewWorkbookWriter(outputfile)

	sh := xlsx.NewSheetWithColumns(cols)
	sw, err := ww.NewSheetWriter(&sh)

	for i := 0; i < 2000; i++ {
		rows, err := db.Query(fmt.Sprintf("SELECT * FROM tweets LIMIT %v OFFSET %v", n, i+1*n))
		if err != nil {
			log.Fatal(err)
		}

		for rows.Next() {
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
		rows.Close()
	}

	err = ww.Close()
	if err != nil {
		panic(err)
	}

	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.WriteHeapProfile(f)
		f.Close()
		return
	}

}
