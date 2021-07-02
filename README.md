# collections

## A Go implementation of List (Key/Value and Indexed), and Table (Rows/Columns)
The *collection* package implements a key/value List and a rows/columns Table.

### List
- Access elements via Map or Indexed Array.
- Elements can be of any type (multiple types in the same List).
- Fast search (a b-tree like search via thread workers). 
- Sort in both directions (asc and desc).
- Includes KeyExists(), ValueExists() methods to avoid duplicates.
- Remove and Insert by key/value or array index.

#### Methods
```go
	Add(k string, v interface{}) error
	Count() int
	Empty()
	KeyExists(k string) bool
	ValueExists(v string) bool
	Get() *[]Element
	GetItem(i int) (Element, error)
	SetItem(i int, v interface{}) error
	GetJSON() string
	Map() map[string]interface{}
	IndexOfKey(key string) int
	IndexOfValue(v interface{}) int
	InsertAt(i int, k string, v interface{}) error
	RemoveAt(i int)
	RemoveByKey(k string)
	RemoveByValue(v interface{})
	SortByKey(order SortOrder)
	SortByValue(order SortOrder)
	SetKey(oldKey string, newkey string) error
	SetValue(k string, v interface{}) error
	GetValue(key string) (interface{}, error)
```

#### Example
```go
var coll = collections.NewCollection()
tx := time.Now()
for i := 0; i < 1000000; i++ {
   k := fmt.Sprintf("%d", i)
   // Add different types
   var v interface{}
   if i%2 == 0 {
      v = fmt.Sprintf("%d Green Dolphin Street", i)
   } else {
      v = i + 1250
   }
   coll.List.Add(k, v)
}
txd := time.Since(tx)
fmt.Println("took:", txd, `to add 1,000,000 elements to the list.`)
fmt.Println(`searching for "Green Dolphin Street 694823"`)

tx = time.Now()
fmt.Println("IndexOf:", coll.List.IndexOf("694823"))
txd = time.Since(tx)
fmt.Println("took:", txd, `to find "Dolphin Street 694823" in the list.`)
Output:
took: 313.756394ms to add 1,000,000 elements to the list.
searching for "Green Dolphin Street 694823"
IndexOf: 694823
took: 4.335141ms to find "Dolphin Street 694823" in the list.

// Iterate thru the list.
for x := 0; x < 10; x++ {
	l, _ := coll.List.GetItem(x)
	fmt.Println("key:", l.Key, "value:", l.Value)
}
```

### Table
Table is a classic representation of a data-table with rows and columns.
- Access rows via Map or Indexed Array. 
- Add a tag for selected rows.

#### Methods (Rows)
```go
AddWithTag(tag string) RowMap
Add() RowMap
Count() int
GetMaps() []RowMap
GetArray() []Row
GetRow(inx int) Row
GetRowMap(i int) RowMap
Clear()
```

#### Example
```go
var coll = collections.NewCollection()
tbl := coll.Table.Create("Test")
tbl.Columns.Add("State")
tbl.Columns.Add("Capital")

oneRow := tbl.Rows.Add()
oneRow["State"] = "Maine"
oneRow["Capital"] = "Augusta"

oneRow = tbl.Rows.Add()
oneRow["State"] = "Oregon"
oneRow["Capital"] = "Salem"

oneRow = tbl.Rows.Add()
oneRow["State"] = "Georgia"
oneRow["Capital"] = "Atlanta"

cols := tbl.Columns.Get()
rows := tbl.Rows.GetMaps()

fmt.Print(strings.Repeat(" ", 4))
for j := 0; j < len(cols); j++ {
   fmt.Print(cols[j].Name, strings.Repeat(" ", 7))
}
fmt.Print("\n")
fmt.Println("---------------------------")

for i := 0; i < len(rows); i++ {
   for j := 0; j < len(cols); j++ {
      fmt.Print(strings.Repeat(" ", 4), rows[i][cols[j].Name], strings.Repeat(" ", 4))
   }
   fmt.Print("\n")
}

Output:
State Capital 
---------------------------
Maine Augusta 
Oregon Salem 
Georgia Atlanta 
```
