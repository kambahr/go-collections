// (c) Kamiar Bahri
package collections

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
)

// IColumn is the column interface.
type IColumn interface {
	Add(name string) *Column
	Get() []Column
	SetColumns(colArry []Column)

	// Clear drops all columns
	Clear()

	Exists(colName string) bool
	Count() int
	InsertAt(pos int, col Column) error

	// GetOccurrence gets a count of number-of-times a values is
	// repeated in a column's data.
	GetOccurrence(colName string, value interface{}, data []interface{}) int

	// GetOccurrenceMatrix reports the number of ccurrence
	// for each value in a column.
	//GetOccurrenceMatrix(colName string) []OccurrenceMatrix
	GetOccurrenceMatrix(colName string, tbl *Table)

	// ResetColTypes re-examins the values in columns to
	// esure that the correct type is set; i.e. when loadng
	// CSV all columns' type may be set to string (e.g
	// "433" vs 433).
	ResetColTypes()

	// GetData retrieves all values of a column.
	GetData(colName string) []interface{}
	ColDataCount(colName string) int
	ColDataNoNULL(colName string) int

	// GetDataDistinct gets all distinct values of a column.
	GetDataDistinct(colName string) ([]interface{}, []interface{})

	setTag(t string)
	getTag() string
}

func (c *Cols) ColDataNoNULL(colName string) int {

	dataCount := 0
	colCnt := len(c.Columns)
	rowCnt := c.Rows.Count()

	for j := 0; j < colCnt; j++ {
		name := c.Columns[j].Name

		if name == colName {
			for i := 0; i < rowCnt; i++ {
				row := c.Rows.GetRow(i)
				v := row
				if v != nil {
					vstr := strings.TrimSpace(fmt.Sprintf("%v", v))
					if vstr != "" {
						dataCount++
					}
				}
			}
		}
	}

	return dataCount
}

func (c *Cols) ColDataCount(colName string) int {

	dataCount := 0
	colCnt := len(c.Columns)
	rowCount := c.Rows.Count()

	for i := 0; i < rowCount; i++ {
		for j := 0; j < colCnt; j++ {
			name := c.Columns[j].Name
			if name == colName {
				dataCount++
			}
		}
	}

	return dataCount
}

type OccurrenceMatrix struct {
	Value    interface{} `json:"value"`
	Occurred int         `json:"occurred"`
}

type Column struct {
	// Tag is used to identify a row. All columns are assinged
	// the same tag to identify one row.
	Tag string `json:"tag"`

	Name string `json:"name"`
	Type string `json:"type"`
}

// Columns is the IColumn interface handler.
type Cols struct {
	Columns []Column
	Rows    IRows

	wrkGrpOccurenceCount int
}

func (c *Cols) GetDataDistinct(colName string) ([]interface{}, []interface{}) {

	var dist []interface{}

	// get the data as-is
	d := c.GetData(colName)

	// create a list with the data; the list
	// does not allow duplicates; and it has a
	// built-in binary search...
	var coll = NewCollection()
	for i := 0; i < len(d); i++ {

		// key is actually the valuoe
		key := fmt.Sprintf("%v", d[i])

		coll.List.Add(key, i)
	}

	for i := 0; i < coll.List.Count(); i++ {
		x, _ := coll.List.GetItem(i)
		dist = append(dist, x.Key)

	}

	return dist, d
}
func (c *Cols) SetColumns(colArry []Column) {
	c.Columns = colArry
	c.Rows.SetColumns(c.Columns)
}
func (c *Cols) GetData(colName string) []interface{} {
	var d []interface{}
	colCnt := len(c.Columns)
	rows := c.Rows.GetRows()

	for i := 0; i < len(rows); i++ {
		for j := 0; j < colCnt; j++ {
			name := c.Columns[j].Name
			if name == colName {
				d = append(d, rows[i][colName])
			}
		}
	}
	return d
}
func (c *Cols) Clear() {
	c.Columns = nil
}
func (c *Cols) Exists(colName string) bool {
	colNameLower := strings.ToLower(colName)
	for i := 0; i < len(c.Columns); i++ {
		if strings.ToLower(c.Columns[i].Name) == colNameLower {
			return true
		}
	}
	return false
}

func (c *Cols) ResetColTypes() {
	for i := 0; i < len(c.Columns); i++ {
		colName := c.Columns[i].Name
		distVal, _ := c.GetDataDistinct(colName)
		if distVal == nil {
			continue
		}

		lenx := len(distVal)
		var cntInt, cntStr, cntDate, cntBool, cntFloat64 int
		for j := 0; j < lenx; j++ {

			// if the columm has originally been defined as string, then reflect.TypeOf()
			// still return string. e.g. Wnat: 443 (int), Have: "443" (string);

			strVal := fmt.Sprintf("%v", distVal[j])

			if _, err := strconv.ParseFloat(strVal, 64); err == nil {
				cntFloat64++
				continue
			}

			_, err := strconv.Atoi(strVal)
			if err == nil {
				cntInt++
				continue
			}

			// Date
			if (strings.Contains(strVal, "/") || strings.Contains(strVal, "-")) &&
				strings.Contains(strVal, ":") && strings.Contains(strVal, " ") {
				_, err := convertStringToTime(strVal)
				if err == nil {
					cntDate++
					continue
				}
			}

			if strings.ToLower(strVal) == "false" || strings.ToLower(strVal) == "true" {
				cntBool++
				continue
			}

			cntStr++
		}

		if lenx == cntDate {
			c.Columns[i].Type = "DateTime"

		} else if lenx == cntStr {
			c.Columns[i].Type = "String"

		} else if lenx == cntInt {
			c.Columns[i].Type = "Integer"

		} else if lenx == cntBool {
			c.Columns[i].Type = "Bool"

		} else if lenx == cntFloat64 {
			c.Columns[i].Type = "Float"
		}
	}
}
func (c *Cols) GetOccurrenceMatrix(colName string, tbl *Table) {

	//-----------------------------
	// UNDONE
	//-----------------------------

	distint, flatData := c.GetDataDistinct(colName)
	distLen := len(distint)
	flatLen := len(flatData)
	ratio := float64(distLen) / float64(flatLen)

	if flatLen == distLen {
		// Unique values, all occurances are equal to 1
		row := tbl.Rows.New()
		row["column_name"] = colName
		row["is_unique"] = 1
		tbl.Rows.UpdateRow(row)
		return
	}
	if distLen == 1 && flatLen > 0 {
		row := tbl.Rows.New()
		row["column_name"] = colName
		row["is_unique"] = 0
		row["n_times_occurred"] = flatData
		row["distinct_to_all_ratio"] = ratio
		tbl.Rows.UpdateRow(row)
		return
	}

	if ratio > 0.99 && flatLen > 10000 {
		// there are only a few that are different
		row := tbl.Rows.New()
		row["column_name"] = colName
		row["is_unique"] = 0
		row["n_times_occurred"] = 1
		row["distinct_to_all_ratio"] = ratio
		tbl.Rows.UpdateRow(row)
		return
	}

	if distLen == 1 && flatLen > 0 {
		row := tbl.Rows.New()
		row["column_name"] = colName
		row["is_unique"] = 0
		row["n_times_occurred"] = flatData
		row["distinct_to_all_ratio"] = ratio
		tbl.Rows.UpdateRow(row)
		return
	}

	for i := 0; i < len(distint); i++ {

		val := distint[i]
		cnt := c.GetOccurrence(colName, val, flatData)

		row := tbl.Rows.New()
		row["column_name"] = colName
		row["distinct_to_all_ratio"] = ratio
		row["value"] = val
		row["is_unique"] = 0
		row["n_times_occurred"] = cnt

		tbl.Rows.UpdateRow(row)
	}
}

func (c *Cols) GetOccurrence(colName string, value interface{}, data []interface{}) int {

	if data == nil {
		data = c.GetData(colName)
	}

	maxRecNoThread := 10000
	c.wrkGrpOccurenceCount = 0
	recordCount := len(data)

	if recordCount < maxRecNoThread {
		c.getOccurrenceWalkthru(value, 0, maxRecNoThread, data, nil)
		return c.wrkGrpOccurenceCount
	}

	var wg sync.WaitGroup
	thrdCnt := 4

	wg.Add(thrdCnt)

	from := 0
	to := recordCount / thrdCnt
	remainder := recordCount % 2

	for i := 0; i < thrdCnt; i++ {
		go c.getOccurrenceWalkthru(value, from, to, data, &wg)

		from = to - 1
		to = (to * 2)

		if i == (thrdCnt-1) && remainder > 0 {
			to += remainder
		}

	}

	wg.Wait()

	return c.wrkGrpOccurenceCount
}
func (c *Cols) getOccurrenceWalkthru(value interface{}, from int, to int, data []interface{}, wg *sync.WaitGroup) {

	if wg != nil {
		defer wg.Done()
	}

	recordCount := len(data)

	if to > recordCount {
		to = recordCount
	}

	for i := from; i < to; i++ {
		if value == data[i] {
			c.wrkGrpOccurenceCount++
		}
	}
}

func isItemDuplicateInArray(e []interface{}, v interface{}) bool {

	for i := 0; i < len(e); i++ {
		if e[i] == v {
			return true
		}
	}

	return false
}

func (c *Cols) InsertAt(pos int, col Column) error {

	origLen := len(c.Columns)
	var colNew []Column

	if pos > origLen || pos < -1 {
		return errors.New("invalid columns position")
	}

	if c.Exists(col.Name) {
		return errors.New("column already exists")
	}

	for i := 0; i < origLen+1; i++ {
		if i == pos {
			colNew = append(colNew, col)
		} else {
			if i < origLen {
				colNew = append(colNew, c.Columns[i])
			}
		}
	}

	// clean up duplicates
	var last []Column
	for i := 0; i < len(colNew); i++ {
		found := false
		for j := 0; j < len(last); j++ {
			if last[j].Name == colNew[i].Name {
				found = true
				break
			}
		}
		if !found {
			last = append(last, colNew[i])
		}
	}

	c.Columns = last

	// Also replace the Row columns
	c.Rows.SetColumns(c.Columns)

	return nil
}
func (c *Cols) Count() int {

	return len(c.Columns) //r.Rows[r.Row[len(r.Row)-1][row_id]]

}
func (c *Cols) getTag() string {
	if len(c.Columns) > 0 {
		return c.Columns[0].Tag // All columns have the same tag
	}
	return ""
}

func (c *Cols) Get() []Column {
	return c.Columns
}

// setTag sets a tag for an row. Note that all columns share the same
// tag to comprise a tag for a row.
func (c *Cols) setTag(t string) {
	for i := 0; i < len(c.Columns); i++ {
		c.Columns[i].Tag = t
	}
}
func (c *Cols) Add(name string) *Column {

	colType := fmt.Sprintf("%v", reflect.TypeOf(name))

	var col Column = Column{
		Name: name,
		Type: colType,
	}

	c.Columns = append(c.Columns, col)

	c.Rows.SetColumns(c.Columns)

	return &col
}
