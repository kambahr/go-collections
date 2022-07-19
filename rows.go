// (c) Kamiar Bahri
package collections

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
)

// rowInterface defines the methods for Row operations.
type IRows interface {

	// New creates an empty row and returns its map.
	New() Row

	// Add adds a row the Rows array.
	Add(row Row)

	SetColumns(cols []Column)

	// GetColumns returs a list of []Column, The first element is a built-in
	// column called _rowid; it holds the index position of the row at the
	// time it was created.
	GetColumns() []Column
	Count() int

	// GetJSON returns a json representation of the entire table.
	GetJSON() string

	GetRows() []Row
	GetRow(rowIndex int) Row
	GetLastRow() Row

	GetRowIndex(row Row) int
	GetLastRowIndex() int
	UpdateRow(irow Row) error

	GetRowJSON(i int) string

	GetRowsByTagName(tagName string) []Row

	SetTag(rowIndex int, tag Tag)
	GetTag(rowIndex int) Tag

	// TODO:
	// RemoveAt(rowINdex int)
	// Remove(row Row)

	AddSharedData(sharedDataItem SharedDataItem) error
	GetSharedData(tagName string) SharedDataItem

	// InsertRecords creates new rows from a two demintional array of string.
	// Example of input is a result-set from reading CSV file.
	InsertRecords(input [][]string, verbose bool)

	// InsertSingleRecord creates a new row from an array of string.
	InsertSingleRecord(input []string)

	// Clear drops all rows.
	Clear()
}

func (r *Rows) Add(row Row) {

	newRow := r.New()

	i := newRow[row_id].(int)

	r.Rows[i] = row
}
func (r *Rows) AddSharedData(sharedDataItem SharedDataItem) error {

	if sharedDataItem.TagName == "" {
		return fmt.Errorf("tag name is blank")
	}
	// the tag-name of the shared-data muste exist in the row-tags' list
	tagCount := len(r.GetRowsByTagName(sharedDataItem.TagName))

	if tagCount < 1 {
		return fmt.Errorf("now rows found by tag name: %s", sharedDataItem.TagName)
	}

	for i := 0; i < len(r.SharedData); i++ {
		if r.SharedData[i].TagName == sharedDataItem.TagName {
			return errors.New("shared data item alreay exists")
		}
	}

	r.SharedData = append(r.SharedData, sharedDataItem)

	return nil
}

func (r *Rows) Clear() {
	r.Rows = make([]Row, 0)
}

func (r *Rows) Count() int {
	lenx := len(r.Rows)
	return lenx
}

// TODO:
// func (r *Rows) Remove(row Row) {
// }
// func (r *Rows) RemoveAt(i int) {
// }

func (r *Rows) createNewRecordsWorker(instance string, from int, to int, input [][]string, verbose bool, wg *sync.WaitGroup) {
	if wg != nil {
		defer wg.Done()
	}

	fmtCount := fmt.Sprintf("%v", formatNumber(int64(to)))

	for i := from; i < to; i++ {
		if verbose {
			fmt.Printf("\rcreating data-table: row %v of %s [%s]", formatNumber(int64(i)), fmtCount, instance)
		}
		r.InsertSingleRecord(input[i])
	}
}

func (r *Rows) GetColumns() []Column {
	return r.Columns
}

func (r *Rows) GetJSON() string {
	b, _ := json.Marshal(r.Rows)
	b = bytes.ReplaceAll(b, []byte(`\"`), []byte(`"`))

	return string(b)
}

func (r *Rows) GetRowJSON(inx int) string {
	b, _ := json.Marshal(r.GetRow(inx))
	b = bytes.ReplaceAll(b, []byte(`\"`), []byte(`"`))

	return string(b)
}

func (r *Rows) GetLastRowIndex() int {
	return len(r.Rows) - 1
}

func (r *Rows) GetRow(indx int) Row {

	if indx < 0 {
		return nil
	}
	for i := 0; i < len(r.Rows); i++ {
		if i == indx {
			return r.Rows[i]
		}
	}

	return nil
}

func (r *Rows) GetLastRow() Row {

	if len(r.Rows) == 0 {
		return nil
	}

	return r.Rows[len(r.Rows)-1]
}

func (r *Rows) GetRows() []Row {
	return r.Rows
}

func (r *Rows) GetRowsByTagName(tagName string) []Row {

	var rows []Row

	for i := 0; i < len(r.Rows); i++ {
		tg := r.Tags[i].Name

		if tg == tagName {
			rows = append(rows, r.Rows[i])
		}
	}

	return rows
}

func (r *Rows) GetRowIndex(row Row) int {
	return row[row_id].(int)
}

func (r *Rows) GetTag(i int) Tag {
	return r.Tags[i]
}

func (r *Rows) GetSharedData(tagName string) SharedDataItem {

	for i := 0; i < len(r.SharedData); i++ {
		if r.SharedData[i].TagName == tagName {
			return r.SharedData[i]
		}
	}

	return SharedDataItem{}
}

// InsertRecords reads a two-dim. string arrary into the Table.
// Note: there is perfomance hit when verbose is on
func (r *Rows) InsertRecords(input [][]string, verbose bool) {
	recordCount := len(input)
	fmtCount := fmt.Sprintf("%v", formatNumber(int64(recordCount)))

	if recordCount < 1000000 {
		for i := 0; i < recordCount; i++ {
			if verbose {
				fmt.Printf("\rcreating data-table: row %v of %s", formatNumber(int64(i)), fmtCount)
			}
			r.InsertSingleRecord(input[i])
		}
	} else {

		var wg sync.WaitGroup
		wg.Add(2)

		half := recordCount / 2
		remainder := recordCount % 2

		from := 0
		to := half
		go r.createNewRecordsWorker("worker 1", from, to, input, false, &wg)

		from = half
		to = (half * 2) + remainder
		go r.createNewRecordsWorker("worker 2", from, to, input, false, &wg)

		wg.Wait()
	}
}

func (r *Rows) InsertSingleRecord(input []string) {

	colLen := len(r.Columns)
	oneRow := r.New()

	for j := col_start_indx; j < colLen; j++ {
		oneRow[r.Columns[j].Name] = input[j]
	}
}

func (r *Rows) New() Row {
	var row Row

	row = make(map[string]interface{}, 1)
	lastInx := r.GetLastRowIndex()

	if lastInx < 0 {
		lastInx = 0
	}

	for {
		if row[row_id] == nil {
			row[row_id] = lastInx
			break
		} else {
			lastInx++
		}
	}

	// Add an empty tag
	var tag = Tag{Name: "", Data: nil}
	r.Tags = append(r.Tags, tag)

	for i := col_start_indx; i < len(r.Columns); i++ {
		row[r.Columns[i].Name] = nil
	}

	r.Rows = append(r.Rows, row)

	return row
}

func (r *Rows) SetColumns(cols []Column) {
	r.Columns = cols
}

func (r *Rows) SetTag(i int, tag Tag) {
	r.Tags[i] = tag
}

func (r *Rows) UpdateRow(row Row) error {

	i := r.GetRowIndex(row)
	if i < 0 || i >= len(r.Rows) {
		return errors.New("out of bound index")
	}

	m := r.GetRow(i)
	for k := 0; k < len(r.Columns); k++ {
		colName := r.Columns[k].Name
		m[colName] = row[colName]
	}

	return nil
}
