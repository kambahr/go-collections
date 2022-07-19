// (c) Kamiar Bahri
package collections

// Row is a map of columns; map[<column name><any value>
type Row map[string]interface{}

// Tag provides additional info that can be added to a row.
// Each has an associated tag, which can be empty.
type Tag struct {
	Name string      // this is not unique; it helps to group related rows
	Data interface{} //optional user-data
}

// SharedDataItem is a data is linked to rows by tag name.
type SharedDataItem struct {
	TagName string
	Data    interface{}
}

// RowHash identifies a row by an MD5 value of its columns.
type RowHash struct {
	MD5   string
	RowID int
}

// Rows defines fields that comprise one Row; and it acts
// as a bridge between the caller and its interface.
type Rows struct {
	Rows    []Row
	Columns []Column
	Tags    []Tag

	// TODO:
	RowHashes []RowHash

	SharedData []SharedDataItem
}
