// (c) Kamiar Bahri
package collections

import (
	"reflect"
)

// Collection defines the List and Table
// interfaces.
type Collection struct {
	List  listInterface
	Table ITable
}

// IColumn is the column interface.
type IColumn interface {
	Add(name string) Column
	Get() []Column
	setTag(t string)
	getTag() string
}

// Columns is an alias for the IColumn interface.
type Columns IColumn

type Column struct {
	// Tag is used to identify a row. All columns are assinged
	// the same tag to identify one row.
	Tag  string       `json:"tag"`
	Name string       `json:"name"`
	Type reflect.Type `json:"type"`
}

// colHdlr is the IColumn interface handler.
type colHdlr struct {
	Columns []Column
}

// getTag adds additional attribes to a column. Each tag applies to
// all columns, which in turn is used to identify one row.
func (c *colHdlr) getTag() string {
	if len(c.Columns) > 0 {
		return c.Columns[0].Tag // All columns have the same tag
	}
	return ""
}

func (c *colHdlr) Get() []Column {
	return c.Columns
}

// setTag sets a tag for an row. Note that all columns share the same
// tag to comprise a tag for a row.
func (c *colHdlr) setTag(t string) {
	for i := 0; i < len(c.Columns); i++ {
		c.Columns[i].Tag = t
	}
}
func (c *colHdlr) Add(name string) Column {
	var col Column = Column{
		Name: name,
		Type: reflect.TypeOf(name),
	}

	c.Columns = append(c.Columns, col)

	return col
}
