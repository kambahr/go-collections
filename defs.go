// (c) Kamiar Bahri
package collections

const maxTableNameLength = 80

type SortOrder int

const (
	Asc SortOrder = iota
	Desc
)

const (
	// row_id is the array-index of a Row. It is only
	// used by IRow to locate a row in the []Row array.
	row_id         = "_rowid_"
	col_start_indx = 0
)
