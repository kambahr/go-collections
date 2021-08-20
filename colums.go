// (c) Kamiar Bahri
package collections

import (
	"fmt"
	"reflect"
)

// IColumn is the column interface.
type IColumn interface {
	Add(name string) *Column
	Get() []Column
	Count() int
	setTag(t string)
	getTag() string
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
}

func (c *Cols) Count() int {
	return len(c.Columns)
}

// getTag adds additional attributes to a column. Each tag applies to
// all columns, which in turn is used to identify one row.
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
	var col Column = Column{
		Name: name,
		Type: fmt.Sprintf("%v", reflect.TypeOf(name)),
	}

	c.Columns = append(c.Columns, col)

	return &col
}
