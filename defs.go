// (c) Kamiar Bahri
package collections

//go:generate stringer -type=SortOrder
type SortOrder int

const (
	Asc SortOrder = iota
	Desc
)
