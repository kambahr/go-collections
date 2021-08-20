// (c) Kamiar Bahri
package collections

import (
	"bytes"
	"compress/gzip"
	"encoding/gob"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
)

// Collection defines the List and Table interfaces.
type Collection struct {
	List    listInterface
	Table   ITable
	Dataset IDataset
}

// IDataset is the Dataset interface (describes the Dataset interface).
type IDataset interface {
	Add(tbl Table) error
	Remove(i int) error
	RemoveByName(tblName string) error
	Serialize() ([]byte, error)
	SerializeToFile(fPath string) error
	Deserialize(data []byte) ([]Table, error)
	DeserializeFromFile(fPath string) ([]Table, error)
}

// Dataset is the handler for the IDatasetHndlr interface.
type Dataset struct {
	Tables []Table
}

func (d *Dataset) DeserializeFromFile(fPath string) ([]Table, error) {

	if !fileOrDirExists(fPath) {
		return nil, errors.New("filedoes not exist")
	}

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

	d.Tables, err = d.Deserialize(data)
	if err != nil {
		return nil, err
	}

	return d.Tables, nil
}

func (d *Dataset) Deserialize(data []byte) ([]Table, error) {

	var err error
	var tx = NewCollection()
	var tbls []Table
	var m map[string][]byte
	buf := bytes.NewReader(data)
	err = gob.NewDecoder(buf).Decode(&m)
	if err != nil && err.Error() != "EOF" && err.Error() != "unexpected EOF" {
		return d.Tables, err
	}

	for tblName := range m {
		t, err := tx.Table.Deserialize(m[tblName])
		if err != nil {
			return tbls, err
		}
		tbls = append(tbls, *t)
	}

	return tbls, nil
}
func (d *Dataset) SerializeToFile(fPath string) error {

	data, err := d.Serialize()
	if err != nil {
		return err
	}

	f, err := os.Create(fPath)
	if err != nil {
		return err
	}
	w := gzip.NewWriter(f)
	w.Write(data)
	w.Close()

	return nil
}

func (d *Dataset) Serialize() ([]byte, error) {
	var b []byte

	m := make(map[string][]byte, 0)
	var tx = NewCollection()
	for i := 0; i < len(d.Tables); i++ {
		tblBytes, _ := tx.Table.Serialize(&d.Tables[i])
		m[d.Tables[i].Name] = tblBytes
	}
	var encoded bytes.Buffer
	encode := gob.NewEncoder(&encoded)
	err := encode.Encode(m)
	if err != nil {
		return b, err
	}

	return encoded.Bytes(), nil
}
func (d *Dataset) RemoveByName(tblName string) error {
	if tblName == "" {
		return errors.New("table name is empty")
	}

	found := false
	for i := 0; i < len(d.Tables); i++ {
		if d.Tables[i].Name == tblName {
			d.Tables[len(d.Tables)-1], d.Tables[i] = d.Tables[i], d.Tables[len(d.Tables)-1]
			found = true
			break
		}
	}

	if !found {
		return errors.New("table not found")
	}

	return nil
}

func (d *Dataset) Remove(i int) error {
	if i < 0 || i > len(d.Tables) {
		return errors.New(fmt.Sprintf("invalid array index: %d", i))
	}

	d.Tables[len(d.Tables)-1], d.Tables[i] = d.Tables[i], d.Tables[len(d.Tables)-1]

	return nil
}
func (d *Dataset) TableExists(tbl *Table) bool {
	if tbl == nil {
		return false
	}
	if len(d.Tables) == 0 {
		return false
	}

	tNameLower := strings.ToLower(tbl.Name)

	for i := 0; i < len(d.Tables); i++ {

		if strings.ToLower(d.Tables[i].Name) == tNameLower {
			return true
		}
	}

	return false
}

func (d *Dataset) Add(tbl Table) error {

	if d.TableExists(&tbl) {
		return errors.New("table already exists")
	}

	d.Tables = append(d.Tables, tbl)

	return nil
}
