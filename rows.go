// (c) Kamiar Bahri
package collections

import (
	"encoding/json"
	"reflect"
	"strings"
)

type IRows rowInterface

// Row is array of row interfaces
type Row []interface{}

type RowMap map[string]interface{}

// rowInterface defines the methods for Row operatins.
type rowInterface interface {
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

// rowHdlr defines fields that comprise one Row; and it acts
// as a bridge between the caller and its interface.
type rowHdlr struct {
	RowMaps []RowMap
	Rows    []Row
	Columns Columns
}

func (r *rowHdlr) GetRowMap(inx int) RowMap {

	for i := 0; i < len(r.RowMaps); i++ {
		if i == inx {
			return r.RowMaps[i]
		}
	}

	return nil
}
func (r *rowHdlr) GetRowJSON(inx int) string {
	jrowm, _ := json.Marshal(r.GetRowMap(inx))

	return strings.ReplaceAll(string(jrowm), `\"`, `"`)
}
func (r *rowHdlr) GetRow(indx int) Row {

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

func (r *rowHdlr) GetArray() []Row {

	cols := r.Columns.Get()

	for i := 0; i < len(r.Rows); i++ {
		for j := 0; j < len(cols); j++ {
			if r.RowMaps[i][cols[j].Name] != nil {
				r.Rows[i][j] = r.RowMaps[i][cols[j].Name]
				cols[j].Type = reflect.TypeOf(r.RowMaps[i][cols[j].Name])
			}
		}
	}

	return r.Rows
}
func (r *rowHdlr) GetJSON() string {
	jrowm, _ := json.Marshal(r.GetMap())

	return strings.ReplaceAll(string(jrowm), `\"`, `"`)
}
func (r *rowHdlr) GetMap() []RowMap {

	cols := r.Columns.Get()

	for i := 0; i < len(r.Rows); i++ {
		for j := 0; j < len(cols); j++ {
			if r.RowMaps[i][cols[j].Name] != nil {
				r.Rows[i][j] = r.RowMaps[i][cols[j].Name]

				// Keep track of the type dymanically.
				cols[j].Type = reflect.TypeOf(r.RowMaps[i][cols[j].Name])
			}
		}
	}

	return r.RowMaps
}

func (r *rowHdlr) Clear() {
	r.RowMaps = make([]RowMap, 0)
}

func (r *rowHdlr) Count() int {
	return len(r.RowMaps)
}

func (r *rowHdlr) AddWithTag(t string) RowMap {
	t = strings.Trim(t, " ")
	if t == "" {
		return r.add("")
	}
	return r.add(t)
}

// Add adds a row. It makes the row available via map and indexed array.
func (r *rowHdlr) Add() RowMap {
	return r.add("")
}

func (r *rowHdlr) add(t string) RowMap {
	var row RowMap

	cols := r.Columns.Get()

	if t != "" {
		r.Columns.setTag(t)
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
