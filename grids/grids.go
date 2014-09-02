package grids

import (
	"database/sql"
	"fmt"
	"io"
	"strconv"

	"code.google.com/p/go.net/html"
	_ "github.com/mattn/go-sqlite3"
	"github.com/psmithuk/xlsx"
)

func AllGrids(db *sql.DB) ([]struct{ URL, Title string }, error) {
	gridsToWrite := []struct{ URL, Title string }{}
	rows, err := db.Query("SELECT url, title FROM _grids WHERE number>0")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var gridURL, gridTitle string
		err = rows.Scan(&gridURL, &gridTitle)
		if err != nil {
			return nil, err
		}
		gridsToWrite = append(gridsToWrite, struct{ URL, Title string }{gridURL, gridTitle})
	}
	return gridsToWrite, nil
}

func GridURL(db *sql.DB, pageNum int) (string, error) {
	row := db.QueryRow("SELECT url FROM _grids WHERE number=?", pageNum)
	var gridURL string
	err := row.Scan(&gridURL)
	return gridURL, err
}

func GridTitle(db *sql.DB, pageNum int) (string, error) {
	row := db.QueryRow("SELECT title FROM _grids WHERE number=?", pageNum)
	var title string
	err := row.Scan(&title)
	return title, err
}

func WriteGridSheet(ww *xlsx.WorkbookWriter, table HTMLTable) error {
	c := []xlsx.Column{}

	for i := uint64(0); i < table.ColNum; i++ {
		c = append(c, xlsx.Column{
			Name:  fmt.Sprintf("Col%v", i),
			Width: 10,
		})
	}

	sh := xlsx.NewSheetWithColumns(c)
	sh.Title = table.Title
	sw, err := ww.NewSheetWriter(&sh)
	if err != nil {
		panic(err)
	}

	// Ghost cells are extra merged cells that need to be inserted as a
	// result of combinations of colspans and rowspans in the grids.
	// This happens because we don't have vertical size information
	// about the grids when dealing with them as a stream.
	ghostCells := make(map[uint64][]uint64)
	var x uint64
	for htmlRow := range table.Rows {
		sheetRow := sh.NewRow()

		// If there should be ghost cells on this row, insert them now
		ghostCellRow, ok := ghostCells[x]
		if ok {
			for _, ghostCellY := range ghostCellRow {
				sheetRow.Cells[ghostCellY] = xlsx.Cell{xlsx.CellTypeInlineString, "", 1, 1}
			}
		}

		var y uint64
		for _, htmlCell := range htmlRow {
			// If a ghost cell is already here, move along one
			for sheetRow.Cells[y].Type == xlsx.CellTypeInlineString {
				y += 1
			}
			sheetRow.Cells[y] = xlsx.Cell{
				Type:    xlsx.CellTypeInlineString,
				Value:   htmlCell.Text,
				Colspan: htmlCell.Colspan,
				Rowspan: htmlCell.Rowspan,
			}

			var i uint64
			for i = 1; i < htmlCell.Rowspan; i++ {
				var j uint64
				for j = 0; j < htmlCell.Colspan; j++ {
					ghostCells[x+i] = append(ghostCells[x+i], y+j)
				}
			}
			y += htmlCell.Colspan
		}

		x += 1
		err = sw.WriteRows([]xlsx.Row{sheetRow})
		if err != nil {
			return err
		}
	}
	return nil
}

type HTMLTable struct {
	ColNum uint64
	Rows   chan HTMLRow
	Title  string
}

type HTMLRow []HTMLCell

func (row HTMLRow) CountCols() uint64 {
	var sum uint64
	for _, cell := range row {
		sum += cell.Colspan
	}
	return sum
}

type HTMLCell struct {
	Text    string
	Rowspan uint64
	Colspan uint64
}

func ParseHTML(f io.Reader, tables chan<- HTMLTable, title string) {
	z := html.NewTokenizer(f)
	tableCount := 1

	for {
		switch z.Next() {
		case html.ErrorToken:
			return

		case html.StartTagToken:
			if z.Token().Data == "table" {
				ParseHTMLTable(z, tables, fmt.Sprintf("%s Table %v", title, tableCount))
				tableCount += 1
			}
		}
	}
}

func ParseHTMLTable(z *html.Tokenizer, tables chan<- HTMLTable, title string) {
	rows := make(chan HTMLRow, 1)
	defer close(rows)

	// Discover the first row to retrieve colnum
findFirstTr:
	for {
		switch z.Next() {
		case html.ErrorToken:
			return
		case html.StartTagToken:
			t := z.Token()
			if t.Data == "tr" {
				ParseHTMLRow(z, rows)
				row := <-rows
				if !IsMetaRow(t.Attr) {
					rows <- row
				}
				tables <- HTMLTable{row.CountCols(), rows, title}
				break findFirstTr
			}
		}
	}

	for {
		switch z.Next() {
		case html.ErrorToken:
			return
		case html.StartTagToken:
			t := z.Token()
			if t.Data == "tr" {
				if !IsMetaRow(t.Attr) {
					ParseHTMLRow(z, rows)
				}
			}
		case html.EndTagToken:
			if z.Token().Data == "table" {
				return
			}
		}
	}
}

func ParseHTMLRow(z *html.Tokenizer, rows chan<- HTMLRow) {
	currentRow := HTMLRow{}
	for {
		switch z.Next() {
		case html.ErrorToken:
			return
		case html.StartTagToken:
			t := z.Token()
			if t.Data == "td" {
				ParseHTMLCell(z, &currentRow, t)
			}
		case html.EndTagToken:
			if z.Token().Data == "tr" {
				rows <- currentRow
				return
			}
		}
	}
}

func ParseHTMLCell(z *html.Tokenizer, currentRowPtr *HTMLRow, t html.Token) {
	rowspan, colspan, err := GetSpans(t.Attr)
	if err != nil {
		panic(fmt.Sprintf("Non numeric span: %v", t.Attr))
	}

	currentCell := HTMLCell{"", rowspan, colspan}
	for {
		switch z.Next() {
		case html.ErrorToken:
			return
		case html.TextToken:
			currentCell.Text = z.Token().Data
		case html.EndTagToken:
			if z.Token().Data == "td" {
				currentRow := *currentRowPtr
				*currentRowPtr = append(currentRow, currentCell)
				return
			}
		}
	}
}

func GetSpans(attributes []html.Attribute) (rowspan, colspan uint64, err error) {
	rowspan = 1
	colspan = 1
	for _, attribute := range attributes {
		if attribute.Key == "colspan" {
			colspanInt, err := strconv.Atoi(attribute.Val)
			if err != nil {
				return 0, 0, err
			}
			colspan = uint64(colspanInt)
		} else if attribute.Key == "rowspan" {
			rowspanInt, err := strconv.Atoi(attribute.Val)
			if err != nil {
				return 0, 0, err
			}
			rowspan = uint64(rowspanInt)
		}
	}
	return rowspan, colspan, nil
}

// Meta rows are not part of the grid data
func IsMetaRow(attributes []html.Attribute) bool {
	for _, attribute := range attributes {
		if attribute.Key == "class" && attribute.Val == "meta_row" {
			return true
		}
	}
	return false
}
