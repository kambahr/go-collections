// (c) Kamiar Bahri
package collections

type Table struct {
	Name    string
	Columns Columns
	Rows    IRows
}

// ITable is the table interface.
type ITable interface {
	Create(name string) *Table
}

// tableHdlr is the handler for the ITable interface.
type tableHdlr struct {
	Table *Table
	Col   Columns
}

// Create initializes an empty table.
func (t *tableHdlr) Create(name string) *Table {
	var tbl Table
	tbl.Name = name

	var col []Column
	tbl.Columns = &colHdlr{col}

	var rowMaps []RowMap
	var rows []Row
	tbl.Rows = &rowHdlr{rowMaps, rows, tbl.Columns}

	return &tbl
}
