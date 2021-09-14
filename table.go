// (c) Kamiar Bahri
package collections

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"encoding/gob"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
)

// ITable is the table interface.
type ITable interface {
	Create(name string) (*Table, error)
	GetJSON(tbl *Table) string

	Serialize(tbl *Table) ([]byte, error)
	Deserialize(data []byte) (*Table, error)

	SerializeToFile(tbl *Table, fPath string) error
	DeserializeFromFile(fPath string) (*Table, error)
}

// tableHdlr is the handler for the ITable interface.
type Table struct {
	Name string
	Cols IColumn
	Rows IRows
}

func (t *Table) SerializeToFile(tbl *Table, fPath string) error {

	data, err := t.Serialize(tbl)
	if err != nil {
		return err
	}

	// Compress before writing to file.
	f, err := os.Create(fPath)
	if err != nil {
		return err
	}
	w := gzip.NewWriter(f)
	w.Write(data)
	w.Close()

	return nil
}
func (t *Table) Deserialize(b []byte) (*Table, error) {

	var err error
	var m []map[string]interface{}

	b, err = base64.StdEncoding.DecodeString(string(b))
	if err != nil {
		return nil, err
	}

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

	tbl, err := t.Create(tblName)
	if err != nil {
		return nil, errors.New(err.Error())
	}
	for k := range m[0] {
		tbl.Cols.Add(k)
	}
	cols := tbl.Cols.Get()

	for i := 0; i < len(m); i++ {
		oneRow := tbl.Rows.Add()
		for j := 0; j < len(cols); j++ {
			oneRow[cols[j].Name] = m[i][cols[j].Name]
		}
	}

	return tbl, nil
}

// Serialze turns a data-table into bytes of gob.
func (t *Table) Serialize(tbl *Table) ([]byte, error) {
	var encoded bytes.Buffer

	rows := tbl.Rows.GetMap()

	encode := gob.NewEncoder(&encoded)
	err := encode.Encode(rows)
	if err != nil {
		return nil, err
	}

	// Add the table name to the top (first n bytes) of the byte array.
	data := appendTableNameToData(tbl.Name, encoded.Bytes())

	s64based := base64.StdEncoding.EncodeToString(data)
	data = []byte(s64based)

	return data, nil
}
func (t *Table) DeserializeFromFile(fPath string) (*Table, error) {

	f, err := os.Open(fPath)
	if err != nil {
		return nil, err
	}
	reader, err := gzip.NewReader(f)
	if err != nil {
		return nil, err
	}
	data, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	reader.Close()
	f.Close()

	tbl, err := t.Deserialize(data)
	if err != nil {
		return nil, err
	}

	return tbl, nil
}

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
func (t *Table) Create(name string) (*Table, error) {

	if len(name) > 80 {
		return nil, errors.New("maximum length for a table name is 80")
	}

	var tbl Table
	tbl.Name = name

	var rowMaps []RowMap
	var rows []Row
	var cols Cols
	var colArry []Column
	tbl.Rows = &Rows{rowMaps, rows, cols, colArry}
	tbl.Cols = &Cols{colArry}

	return &tbl, nil
}

func (t *Table) GetJSON(tbl *Table) string {
	cols := tbl.Cols.Get()
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
