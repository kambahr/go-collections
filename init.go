// (c) Kamiar Bahri
package collections

// NewCollection makes a new instance of collections. It includes
// List, and Table (Rows/Columns)
func NewCollection() *Collection {
	var c Collection

	var list []Element
	m := make(map[string]interface{}, 0)
	c.List = &listHdlr{list, -1, false, m}

	var col []Column

	var t *Table
	c.Table = &tableHdlr{t, &colHdlr{col}}

	return &c
}
