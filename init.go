// (c) Kamiar Bahri
package collections

// NewCollection makes a new instance of collections. It includes
// List, and Table (Rows/Columns)
func NewCollection() *Collection {
	var c Collection

	var list []Element
	m := make(map[string]interface{}, 0)
	c.List = &listHdlr{list, -1, false, m}

	var col *Cols
	var rows *Rows
	c.Table = &Table{"", col, rows}

	var tblArry []Table
	c.Dataset = &Dataset{tblArry}

	return &c
}
