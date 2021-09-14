// (c) Kamiar Bahri
package collections

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
)

// Row is array of row interfaces
type Row []interface{}

type RowMap map[string]interface{}

// rowInterface defines the methods for Row operatins.
type IRows interface {
	AddWithTag(tag string) RowMap
	Add() RowMap
	Count() int
	GetMap() []RowMap
	GetJSON() string
	GetArray() []Row
	GetRow(inx int) Row
	GetRowMap(i int) RowMap
	GetRowJSON(i int) string
	Clear()
}

// Rows defines fields that comprise one Row; and it acts
// as a bridge between the caller and its interface.
type Rows struct {
	RowMaps []RowMap
	Rows    []Row
	Cols    Cols
	Columns []Column
}

func (r *Rows) GetRowMap(inx int) RowMap {
	for i := 0; i < len(r.RowMaps); i++ {
		if i == inx {
			return r.RowMaps[i]
		}
	}

	return nil
}
func (r *Rows) GetRowJSON(inx int) string {
	b, _ := json.Marshal(r.GetRowMap(inx))
	b = bytes.ReplaceAll(b, []byte(`\"`), []byte(`"`))

	return string(b)
}
func (r *Rows) GetRow(indx int) Row {

	if indx < 0 {
		return nil
	}

	for i := 0; i < len(r.Rows); i++ {
		if i == indx {
			return r.Rows[i]
		}
	}

	return nil
}

func (r *Rows) GetArray() []Row {

	cols := r.Cols.Get()

	for i := 0; i < len(r.Rows); i++ {
		for j := 0; j < len(cols); j++ {
			if r.RowMaps[i][cols[j].Name] != nil {
				r.Rows[i][j] = r.RowMaps[i][cols[j].Name]
				cols[j].Type = fmt.Sprintf("%v", reflect.TypeOf(r.RowMaps[i][cols[j].Name]))
			}
		}
	}

	return r.Rows
}
func (r *Rows) GetJSON() string {
	b, _ := json.Marshal(r.GetMap())
	b = bytes.ReplaceAll(b, []byte(`\"`), []byte(`"`))

	return string(b)
}
func (r *Rows) GetMap() []RowMap {

	cols := r.Cols.Get()

	for i := 0; i < len(r.Rows); i++ {
		for j := 0; j < len(cols); j++ {
			if r.RowMaps[i][cols[j].Name] != nil {
				r.Rows[i][j] = r.RowMaps[i][cols[j].Name]

				// Keep track of the col type
				cols[j].Type = fmt.Sprintf("%v", reflect.TypeOf(r.RowMaps[i][cols[j].Name]))
			}
		}
	}

	return r.RowMaps
}

func (r *Rows) Clear() {
	r.RowMaps = make([]RowMap, 0)
}

func (r *Rows) Count() int {
	return len(r.RowMaps)
}

func (r *Rows) AddWithTag(t string) RowMap {
	t = strings.Trim(t, " ")
	if t == "" {
		return r.add("")
	}
	return r.add(t)
}

// Add adds a row. It makes the row available via map and indexed array.
func (r *Rows) Add() RowMap {
	return r.add("")
}

func (r *Rows) add(t string) RowMap {
	var row RowMap

	cols := r.Cols.Get()

	if t != "" {
		r.Cols.setTag(t)
	}
	// Mapped
	row = make(map[string]interface{}, 1)

	for i := 0; i < len(cols); i++ {
		row[cols[i].Name] = nil
	}
	r.RowMaps = append(r.RowMaps, row)

	// Indexed
	rowi := make(Row, len(cols))
	for i := 0; i < len(cols); i++ {
		rowi[i] = nil
	}
	r.Rows = append(r.Rows, rowi)

	return row
}
