// (c) Kamiar Bahri
package collections

const maxTableNameLength = 80

//go:generate stringer -type=SortOrder
type SortOrder int

const (
	Asc SortOrder = iota
	Desc
)

//go:generate stringer -type=Field
type Field int

const (
	Name Field = iota
	X25
)
