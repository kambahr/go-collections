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

#### Example
```go
var coll = collections.NewCollection()
	tbl, _ := coll.Table.Create("state-capital")
	tbl.Cols.Add("state")
	tbl.Cols.Add("capital")

	oneRow := tbl.Rows.New()
	oneRow["state"] = "Maine"
	oneRow["capital"] = "Augusta"

	oneRow = tbl.Rows.New()
	oneRow["state"] = "Georgia"
	oneRow["capital"] = "Atlanta"

	rows := tbl.Rows.GetRows()
	cols := tbl.Cols.Get()
	fmt.Printf("Stat%sCapital\n", strings.Repeat(" ", 5))
	fmt.Println(strings.Repeat("-", 18))
	for i := 0; i < len(rows); i++ {
		v := rows[i][cols[0].Name].(string)
		s := strings.Repeat(" ", 9)
		d := len(s) - len(v)
		s = fmt.Sprintf("%s%s", v, strings.Repeat(" ", d))
		fmt.Println(s, rows[i][cols[1].Name])
	}


Stat     Capital
------------------
Maine     Augusta
Georgia   Atlanta

// Row examples:

// Get a row
tbl.Rows.GetRows()[0][10] // row, col indexes

// Get a row
tbl.Rows.GetMap()[0]       // row index, map

// Get a single column in a row
tbl.Rows.GetMap()[0]["my-column-name"] // row index, map-string-value
```
