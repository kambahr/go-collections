// (c) Kamiar Bahri
package collections

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"strings"
)

type Table struct {
	Name    string
	Columns Columns
	Rows    IRows
}

// ITable is the table interface.
type ITable interface {
	Create(name string) *Table
	GetJSON(tbl *Table) string
	Serialize(tbl *Table) ([]byte, error)
	Deserialize(b []byte) (*Table, error)
}

// tableHdlr is the handler for the ITable interface.
type tableHdlr struct {
	Table *Table
	Col   Columns
}

func (t *tableHdlr) Deserialize(b []byte) (*Table, error) {

	var m []map[string]interface{}

	tblName, b, err := getTableNameFromData(b)
	if err != nil {
		return nil, err
	}

	buf := bytes.NewReader(b)
	err = gob.NewDecoder(buf).Decode(&m)
	if err != nil && err.Error() != "EOF" {
		return nil, err
	}

	if m == nil || len(m) == 0 {
		return nil, errors.New("no rows found")
	}

	tbl := t.Create(tblName)
	for k := range m[0] {
		tbl.Columns.Add(k)
	}
	cols := tbl.Columns.Get()

	for i := 0; i < len(m); i++ {
		oneRow := tbl.Rows.Add()
		for j := 0; j < len(cols); j++ {
			oneRow[cols[j].Name] = m[i][cols[j].Name]
		}
	}

	return tbl, nil
}

// Serialze turns a data-table into bytes of gob.
func (t *tableHdlr) Serialize(tbl *Table) ([]byte, error) {
	var encoded bytes.Buffer

	rows := tbl.Rows.GetMap()

	encode := gob.NewEncoder(&encoded)
	err := encode.Encode(rows)
	if err != nil {
		return nil, err
	}

	// Add the table name to the top (first n bytes) of the byte array.
	data := appendTableNameToData(tbl.Name, encoded.Bytes())

	return data, nil
}

const maxTableNameLength = 80

func getTableNameFromData(data []byte) (string, []byte, error) {
	var tblName string

	// The first n bytes are reserved for the table name
	dataLen := len(data)

	if dataLen < maxTableNameLength {
		return "", nil, errors.New(fmt.Sprintf("data lentegth is < %d", maxTableNameLength))
	}

	tnmB := make([]byte, maxTableNameLength)
	for i := 0; i < maxTableNameLength; i++ {
		if data[i] == 0 {
			break
		}
		tblName = fmt.Sprintf("%s%s", tblName, string(data[i]))
	}
	tnmB = bytes.TrimSpace(tnmB)

	bu := new(bytes.Buffer)

	for i := maxTableNameLength; i < len(data); i++ {
		bu.WriteByte(data[i])
	}

	return tblName, bu.Bytes(), nil
}
func appendTableNameToData(tName string, data []byte) []byte {
	var tblNameB [maxTableNameLength]byte
	tblName := []byte(tName)
	bu := new(bytes.Buffer)
	for i := 0; i < len(tblName); i++ {
		tblNameB[i] = tblName[i]
	}
	// Wirte the n bytes
	for i := 0; i < len(tblNameB); i++ {
		bu.WriteByte(tblNameB[i])
	}
	// Add the data
	for i := 0; i < len(data); i++ {
		bu.WriteByte(data[i])
	}
	return bu.Bytes()
}

// Create initializes an empty table.
func (t *tableHdlr) Create(name string) *Table {
	var tbl Table
	tbl.Name = name

	var col []Column
	tbl.Columns = &colHdlr{col}

	var rowMaps []RowMap
	var rows []Row
	tbl.Rows = &rowHdlr{rowMaps, rows, tbl.Columns}

	return &tbl
}

func (t *tableHdlr) GetJSON(tbl *Table) string {
	cols := tbl.Columns.Get()
	rows := tbl.Rows.GetMap()

	var jsnArry []string

	for i := 0; i < len(rows); i++ {
		var sa []string
		for j := 0; j < len(cols); j++ {
			v := rows[i][cols[j].Name]
			if fmt.Sprintf("%v", cols[j].Type) == "string" {
				v = fmt.Sprintf(`"%v"`, v)
			}
			sa = append(sa, fmt.Sprintf(`"%s":%v`, cols[j].Name, v))
		}
		oneJsn := fmt.Sprintf(`{%s}`, strings.Join(sa, ","))
		jsnArry = append(jsnArry, oneJsn)
	}
	allJson := fmt.Sprintf("{[%s]}", strings.Join(jsnArry, ","))

	return allJson
}
