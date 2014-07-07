package main

import (
	"bufio"
	"database/sql"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strconv"

	_ "github.com/mattn/go-sqlite3"
	"github.com/psmithuk/xlsx"
)

func ColumnTypes(db *sql.DB, tablename string) ([]string, *[]interface{}, []interface{}, error) {
	rows, err := db.Query(fmt.Sprintf("PRAGMA TABLE_INFO(%s)", tablename))
	if err != nil {
		return nil, nil, nil, err
	}

	values := []interface{}{}
	valuesPointers := []interface{}{}
	columns := []string{}

	for rows.Next() {

		var type_, name string
		var x interface{}
		err = rows.Scan(&x, &name, &type_, &x, &x, &x)
		if err != nil {
			return nil, nil, nil, err
		}

		columns = append(columns, name)

		var item interface{}

		switch type_ {
		case "text":
			// item = sql.RawBytes{}
		}

		// valuesPointers[index] = &values[index]
		values = append(values, item)
		valuesPointers = append(valuesPointers, &values[len(values)-1])
	}

	if rows.Err() != nil {
		return nil, nil, nil, err
	}

	return columns, &values, valuesPointers, nil
}

func main() {
	db, err := sql.Open("sqlite3", "scraperwiki.sqlite")
	if err != nil {
		log.Fatal("db, err :=", db, err)
	}

	// cols, values, scanArgs, err := ColumnTypes(db, "tweets")
	// if err != nil {
	//  log.Fatal(err)
	// }

	rowCount, err := db.Query("SELECT COUNT(*) FROM tweets")
	if err != nil {
		log.Fatal(err)
	}

	rowCount.Scan(os.Stdout)

	cols, _ := rowCount.Columns()
	countValues := make([]interface{}, len(cols))
	scanArgs := make([]interface{}, len(cols))
	for i := range countValues {
		scanArgs[i] = &countValues[i]
	}

	rowCount.Next()
	rowCount.Scan(scanArgs...)

	rowNum := countValues[0].(int64)
	_ = rowNum

	rows, err := db.Query("SELECT * FROM tweets limit 10")
	if err != nil {
		log.Fatal(err)
	}

	cols, _ = rows.Columns()
	values := make([]interface{}, len(cols))
	scanArgs = make([]interface{}, len(cols))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	// c := 0

	out := csv.NewWriter(os.Stdout)
	_ = out

	// r := make([]string, len(values))

	// rr = make([]xlsx.Cell, len(values))

	var c []xlsx.Column

	for _, colName := range cols {
		c = append(c, xlsx.Column{Name: colName, Width: 10})
	}

	outputfile, err := os.Create("test.xlsx")
	w := bufio.NewWriter(outputfile)
	ww := xlsx.NewWorkbookWriter(w)

	sh := xlsx.NewSheetWithColumns(c)
	sw, err := ww.NewSheetWriter(&sh)

	for rows.Next() {

		err = rows.Scan(scanArgs...)
		if err != nil {
			panic(err)
		}

		r := sh.NewRow()

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
			default:
				r.Cells[i] = xlsx.Cell{
					Type:  xlsx.CellTypeInlineString,
					Value: fmt.Sprintf("%s", v),
				}

				_ = `
			case []byte:
				_ = v
				// out.Write(fmt.Sprint(v))
				//r[i] = string(v)
				//os.Stdout.Write(v)
			default:
				_ = v
				// out.Write(fmt.Sprint(v))
				//r[i] = fmt.Sprint(v)
				//fmt.Fprintln(os.Stdout, v)
            `
			}

		}
		err = sw.WriteRows([]xlsx.Row{r})
	}
	rows.Close()

	err = ww.Close()
	defer w.Flush()

	if err != nil {
		panic(err)
	}

	// log.Printf("Queried %v rows", c)

	// cols, err := rows.Columns()
	// if err != nil {
	//  log.Fatal(err)
	// }
	// values := make([]interface{}, len(cols))
	// scanArgs := make([]interface{}, len(cols))
	// for i := range values {
	//  scanArgs[i] = &values[i]
	// }

	// rows.Next()
	// err = rows.Scan(scanArgs...)
	// if err != nil {
	//  log.Fatal(err)
	// }

	// // result := make([]sql.RawBytes, len(cols))
	// // resultp := make([]interface{}, len(cols))
	// // for i := range result {
	// //   resultp[i] = &result[i]
	// // }

	// // rows.Next()
	// // err = rows.Scan(resultp...)
	// // if err != nil {
	// //   log.Fatal(err)
	// // }

	// for i, c := range cols {
	//  v := values[i]
	//  _, ok := v.([]byte)
	//  if ok {
	//      values[i] = string(v.([]byte))
	//  }
	//  log.Printf("%30s %10T %v", c, values[i], values[i])
	// }
}
