// (c) Kamiar Bahri
package collections

// Collection defines the List and Table
// interfaces.
type Collection struct {
	List    listInterface
	Table   ITable
	Dataset IDataset
}

//----------------------------------------
type Dataset struct {
	//Tables []Table
	Tables listHdlr
}

// IDataset is the Dataset interface.
type IDataset interface {
	Add(tbl *Table)
	Get() []Element

	Base() *Dataset
}

// IDatasetHndlr is the handler for the IDatasetHndlr interface.
type IDatasetHndlr struct {
	Dataset *Dataset
}

// Base lets the caller get/set fields in the Dataset type.
func (i *IDatasetHndlr) Base() *Dataset {
	return i.Dataset
}

func (i *IDatasetHndlr) Get() []Element {
	return i.Dataset.Tables
}

func (i *IDatasetHndlr) Add(tbl *Table) {
	i.Dataset.List.Add(tbl.Name, tbl)
}
