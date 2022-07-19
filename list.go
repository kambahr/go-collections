// (c) Kamiar Bahri
package collections

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"sync"
)

// Element is a key/value structure that holds an item in the list.
type Element struct {
	Key   string      `json:"key"`
	Value interface{} `json:"value"`
}

// listHdlr is handles listInterface.
type listHdlr struct {
	List []Element

	// searchResultIndex is used internally to mark the
	// success of an IndexOf operation via workers.
	searchResultIndex int

	AllowDuplicates bool

	listMap map[string]interface{}
}

// listInterface defines the List.
type listInterface interface {
	Add(k string, v interface{}) error
	Count() int
	Empty()
	KeyExists(k string) bool
	ValueExists(v string) bool
	Get() *[]Element
	Set(e []Element)
	GetItem(i int) (Element, error)
	SetItem(i int, v interface{}) error
	GetJSON() string
	GetMap() map[string]interface{}
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
	Serialize() ([]byte, error)
	SerializeToFile(fPath string) error
	Deserialize(b []byte) ([]Element, error)
	DeserializeFromFile(fPath string) ([]Element, error)
}

// Add adds an item to the top of the list.
func (c *listHdlr) Add(k string, v interface{}) error {

	if k == "" {
		return errors.New("key cannot be blank")
	}
	if !c.AllowDuplicates {
		if c.KeyExists(k) {
			return errors.New("item already exists")
		}
	}

	var e Element
	e.Key = k
	e.Value = v
	c.List = append(c.List, e)

	c.listMap[k] = v

	return nil
}

// searchArrayKey is a worker simulating a binary search.
func (c *listHdlr) searchArrayKey(from int, to int, k string, wg *sync.WaitGroup) {

	defer wg.Done()

	if c.searchResultIndex > -1 {
		return
	}

	b := to - 1

	for t := from; t < to; t++ {

		// It appears to be faster to check this on the top
		// of the loop, rather than an OR conidtion.
		if c.searchResultIndex > -1 {
			// found by another worker
			return
		}

		// Check from the top
		if c.List[t].Key == k {
			// found by this worker
			c.searchResultIndex = t
			return
		}

		// Check from the bottom
		if c.List[b].Key == k {
			// found by this worker
			c.searchResultIndex = b
			return
		}
		b--
	}
}

// searchArrayValue is a worker simulating a binary search.
// The main reason for using array and direct comparison is the
// < or > comp may be off for strings.
func (c *listHdlr) searchArrayValue(from int, to int, v interface{}, wg *sync.WaitGroup) {

	defer wg.Done()

	if c.searchResultIndex > -1 {
		return
	}

	b := to - 1

	for t := from; t < to; t++ {

		// It appears to be faster to check this on the top
		// of the loop, rather than an OR conidtion.
		if c.searchResultIndex > -1 {
			// found by another worker
			return
		}

		// Check from the top
		if c.List[t].Value == v {
			// found by this worker
			c.searchResultIndex = t
			return
		}

		// Check from the bottom
		if c.List[b].Value == v {
			// found by this worker
			c.searchResultIndex = b
			return
		}
		b--
	}
}

// IndexOfKey finds the index position of a matching key in the Element array.
// It simulates a binary-tree like search, via three workers.
func (c *listHdlr) IndexOfKey(k string) int {

	var wg sync.WaitGroup

	c.searchResultIndex = -1

	count := len(c.List)

	if count == 0 {
		return -1
	}

	// Size of < 100 is not significant enough
	// to create workers.
	if count <= 100 {
		for i := 0; i < count; i++ {
			if c.List[i].Key == k {
				return i
			}
		}

		return -1
	}

	// left, mid, and right
	l := count / 3
	m := (l + l)
	remainder := count % 3
	r := l + remainder

	wg.Add(3)
	go c.searchArrayKey(0, l, k, &wg)
	go c.searchArrayKey(l, m, k, &wg)
	go c.searchArrayKey(r, count, k, &wg)

	wg.Wait()

	result := c.searchResultIndex

	// reset
	c.searchResultIndex = -1

	return result
}

// IndexOfValue finds the index position of a matching value in the Element array.
// It simulates a binary-tree like search, via three workers.
func (c *listHdlr) IndexOfValue(v interface{}) int {

	var wg sync.WaitGroup

	c.searchResultIndex = -1

	count := len(c.List)

	if count == 0 {
		return -1
	}

	// Size of < 100 is not significant enough
	// to create workers.
	if count <= 100 {
		for i := 0; i < count; i++ {
			if c.List[i].Value == v {
				return i
			}
		}

		return -1
	}

	// left, mid, and right
	l := count / 3
	m := (l + l)
	remainder := count % 3
	r := l + remainder

	wg.Add(3)
	go c.searchArrayValue(0, l, v, &wg)
	go c.searchArrayValue(l, m, v, &wg)
	go c.searchArrayValue(r, count, v, &wg)

	wg.Wait()

	result := c.searchResultIndex

	// reset
	c.searchResultIndex = -1

	return result
}

func (c *listHdlr) SetItem(i int, v interface{}) error {

	l := len(c.List)

	if i < 0 || i >= l {
		return errors.New("not found")
	}

	c.List[i].Value = v

	return nil
}

// GetItem returns a value by its index position.
func (c *listHdlr) GetItem(i int) (Element, error) {

	var h Element
	l := len(c.List)

	if i < 0 || i >= l {
		return h, errors.New("not found")
	}

	return c.List[i], nil
}

// GetValue returns a value by its key.
func (c *listHdlr) GetValue(k string) (interface{}, error) {

	v := c.GetMap()[k]

	if v == nil {
		return nil, errors.New("not found")
	}

	return v, nil
}

// GetJSON retuns a json string of the entire list.
func (c *listHdlr) GetJSON() string {
	if len(c.List) == 0 {
		return "{}"
	}

	b, _ := json.Marshal(c.List)
	b = bytes.ReplaceAll(b, []byte(`\"`), []byte(`"`))

	// on error, the return will be nil (and not {}).

	return string(b)
}

// Count returns the count of the list
func (c *listHdlr) Count() int {
	return len(c.List)
}

// Map returns a map of key/value of the entire list.
func (c *listHdlr) GetMap() map[string]interface{} {
	return c.listMap
}

func (c *listHdlr) SetKey(oldKey string, newKey string) error {
	if c.listMap[newKey] != nil {
		return errors.New("key already exist")
	}

	i := c.IndexOfKey(oldKey)
	c.List[i].Key = newKey
	c.rebuildMap()

	return nil
}

// SetValue modifies an existing item.
func (c *listHdlr) SetValue(k string, v interface{}) error {

	i := c.IndexOfKey(k)

	if i > -1 {
		c.List[i].Value = v
		return nil
	}

	return errors.New("not found")
}

// Empty clears the list.
func (c *listHdlr) Empty() {
	c.List = make([]Element, 0)
}

// RemoveAt deletes an element from the list by its value.
func (c *listHdlr) RemoveAt(i int) {

	if i < 0 {
		return
	}

	c.List = remove(c.List, i)
	c.rebuildMap()
}

// InsertAt adds an element to the list after an index position.
func (c *listHdlr) InsertAt(i int, k string, v interface{}) error {

	if k == "" {
		return errors.New("key cannot be empty")
	}
	if i < 0 || i > (len(c.List)+1) {
		return errors.New(fmt.Sprintf("%d is out of bound", i))
	}
	if c.KeyExists(k) {
		return errors.New(fmt.Sprintf("%s already exists", k))
	}

	// Add the value to the map and rebuild the list
	c.listMap[k] = v

	// The order of items in the map are the same as the ones in the list:
	c.List = make([]Element, len(c.listMap))
	for i := 0; i < len(c.List); i++ {
		if c.List[i].Key == "" {
			c.List[i].Key = k
			c.List[i].Value = v
		}
		c.listMap[c.List[i].Key] = c.List[i].Value
	}

	return nil
}

// RemoveByValue deletes an element from the list by its value.
func (c *listHdlr) RemoveByValue(v interface{}) {

	i := c.IndexOfValue(v)

	if i > -1 {
		c.List = remove(c.List, i)
		c.rebuildMap()
	}
}

// RemoveByKey deletes an element from the list by its key.
func (c *listHdlr) RemoveByKey(k string) {

	i := c.IndexOfKey(k)

	if i > -1 {
		c.List = remove(c.List, i)
		c.rebuildMap()
	}
}

// rebuildMap re-creates a map of the []Elements.
func (c *listHdlr) rebuildMap() {
	c.listMap = make(map[string]interface{})
	if len(c.List) > 0 {
		for i := 0; i < len(c.List); i++ {
			c.listMap[c.List[i].Key] = c.List[i].Value
		}
	}
}

// remove drops an item from the Element array.
func remove(e []Element, i int) []Element {
	e[len(e)-1], e[i] = e[i], e[len(e)-1]
	return e[:len(e)-1]
}

// KeyExists checks the map of the list to see if the key exists.
func (c *listHdlr) KeyExists(k string) bool {

	if c.GetMap()[k] != nil {
		return true
	}

	return false
}

// ValueExists checks to see if a value exists.
func (c *listHdlr) ValueExists(v string) bool {

	return c.IndexOfValue(v) > -1
}

// SortByValue sorts the list by its value.
// asc is the default sort order.
func (c *listHdlr) SortByValue(order SortOrder) {
	if order == Desc {
		for j := 0; j < len(c.List); j++ {
			for i := len(c.List) - 1; i > 0; i-- {
				hitTest := false
				if fmt.Sprintf("%v", reflect.TypeOf(c.List[i].Value)) == "int" &&
					fmt.Sprintf("%v", reflect.TypeOf(c.List[i-1].Value)) == "int" {
					hitTest = c.List[i].Value.(int) > c.List[i-1].Value.(int)

				} else if fmt.Sprintf("%v", reflect.TypeOf(c.List[i].Value)) == "string" &&
					fmt.Sprintf("%v", reflect.TypeOf(c.List[i-1].Value)) == "string" {
					hitTest = c.List[i].Value.(string) > c.List[i-1].Value.(string)

				} else if fmt.Sprintf("%v", reflect.TypeOf(c.List[i].Value)) == "float64" &&
					fmt.Sprintf("%v", reflect.TypeOf(c.List[i-1].Value)) == "float64" {
					hitTest = c.List[i].Value.(float64) > c.List[i-1].Value.(float64)

				} else if fmt.Sprintf("%v", reflect.TypeOf(c.List[i].Value)) == "float32" &&
					fmt.Sprintf("%v", reflect.TypeOf(c.List[i-1].Value)) == "float32" {
					hitTest = c.List[i].Value.(float32) > c.List[i-1].Value.(float32)

				} else if fmt.Sprintf("%v", reflect.TypeOf(c.List[i].Value)) == "uint" &&
					fmt.Sprintf("%v", reflect.TypeOf(c.List[i-1].Value)) == "uint" {
					hitTest = c.List[i].Value.(uint) > c.List[i-1].Value.(uint)

				} else if fmt.Sprintf("%v", reflect.TypeOf(c.List[i].Value)) == "uint64" &&
					fmt.Sprintf("%v", reflect.TypeOf(c.List[i-1].Value)) == "uint64" {
					hitTest = c.List[i].Value.(uint64) > c.List[i-1].Value.(uint64)

				} else if fmt.Sprintf("%v", reflect.TypeOf(c.List[i].Value)) == "uint32" &&
					fmt.Sprintf("%v", reflect.TypeOf(c.List[i-1].Value)) == "uint32" {
					hitTest = c.List[i].Value.(uint32) > c.List[i-1].Value.(uint32)

				} else if fmt.Sprintf("%v", reflect.TypeOf(c.List[i].Value)) == "byte" &&
					fmt.Sprintf("%v", reflect.TypeOf(c.List[i-1].Value)) == "byte" {
					hitTest = c.List[i].Value.(byte) > c.List[i-1].Value.(byte)
				}

				if hitTest {
					c.List[i].Key, c.List[i-1].Key = c.List[i-1].Key, c.List[i].Key
					c.List[i].Value, c.List[i-1].Value = c.List[i-1].Value, c.List[i].Value
				}
			}
		}
	} else {
		for j := 0; j < len(c.List); j++ {
			for i := 0; i < len(c.List)-1; i++ {
				hitTest := false
				if fmt.Sprintf("%v", reflect.TypeOf(c.List[i].Value)) == "int" &&
					fmt.Sprintf("%v", reflect.TypeOf(c.List[i+1].Value)) == "int" {
					hitTest = c.List[i].Value.(int) > c.List[i+1].Value.(int)

				} else if fmt.Sprintf("%v", reflect.TypeOf(c.List[i].Value)) == "string" &&
					fmt.Sprintf("%v", reflect.TypeOf(c.List[i+1].Value)) == "string" {
					hitTest = c.List[i].Value.(string) > c.List[i+1].Value.(string)

				} else if fmt.Sprintf("%v", reflect.TypeOf(c.List[i].Value)) == "float64" &&
					fmt.Sprintf("%v", reflect.TypeOf(c.List[i+1].Value)) == "float64" {
					hitTest = c.List[i].Value.(float64) > c.List[i+1].Value.(float64)

				} else if fmt.Sprintf("%v", reflect.TypeOf(c.List[i].Value)) == "float32" &&
					fmt.Sprintf("%v", reflect.TypeOf(c.List[i+1].Value)) == "float32" {
					hitTest = c.List[i].Value.(float32) > c.List[i+1].Value.(float32)

				} else if fmt.Sprintf("%v", reflect.TypeOf(c.List[i].Value)) == "uint" &&
					fmt.Sprintf("%v", reflect.TypeOf(c.List[i+1].Value)) == "uint" {
					hitTest = c.List[i].Value.(uint) > c.List[i+1].Value.(uint)

				} else if fmt.Sprintf("%v", reflect.TypeOf(c.List[i].Value)) == "uint64" &&
					fmt.Sprintf("%v", reflect.TypeOf(c.List[i+1].Value)) == "uint64" {
					hitTest = c.List[i].Value.(uint64) > c.List[i+1].Value.(uint64)

				} else if fmt.Sprintf("%v", reflect.TypeOf(c.List[i].Value)) == "uint32" &&
					fmt.Sprintf("%v", reflect.TypeOf(c.List[i+1].Value)) == "uint32" {
					hitTest = c.List[i].Value.(uint32) > c.List[i+1].Value.(uint32)

				} else if fmt.Sprintf("%v", reflect.TypeOf(c.List[i].Value)) == "byte" &&
					fmt.Sprintf("%v", reflect.TypeOf(c.List[i+1].Value)) == "byte" {
					hitTest = c.List[i].Value.(byte) > c.List[i+1].Value.(byte)
				}

				if hitTest {
					c.List[i].Key, c.List[i+1].Key = c.List[i+1].Key, c.List[i].Key
					c.List[i].Value, c.List[i+1].Value = c.List[i+1].Value, c.List[i].Value
				}
			}
		}
	}
}

// SortByKey sorts the list by its key.
// asc is the default sort order.
func (c *listHdlr) SortByKey(order SortOrder) {

	count := len(c.List)

	if order == Desc {
		for j := 0; j < count; j++ {
			for i := count - 1; i >= 1; i-- {
				if c.List[i].Key > c.List[i-1].Key {
					c.List[i].Key, c.List[i-1].Key = c.List[i-1].Key, c.List[i].Key
					c.List[i].Value, c.List[i-1].Value = c.List[i-1].Value, c.List[i].Value
				}
			}
		}
	} else {
		for j := 0; j < (count); j++ {
			for i := 0; i < count-1; i++ {
				if c.List[i].Key > c.List[i+1].Key {
					c.List[i].Key, c.List[i+1].Key = c.List[i+1].Key, c.List[i].Key
					c.List[i].Value, c.List[i+1].Value = c.List[i+1].Value, c.List[i].Value
				}
			}
		}
	}

	c.rebuildMap()
}

// Set sets the main list to the one passed in the arg.
func (c *listHdlr) Set(e []Element) {
	c.List = e
}

// Get returns the entire list.
func (c *listHdlr) Get() *[]Element {
	return &c.List
}

func (c *listHdlr) Deserialize(b []byte) ([]Element, error) {

	var err error
	var m map[string]interface{}

	b, err = base64.StdEncoding.DecodeString(string(b))
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(b, &m)
	if err != nil {
		return nil, err
	}

	if m == nil || len(m) == 0 {
		return nil, errors.New("no items found")
	}

	var e []Element

	for k, v := range m {
		var elm Element
		elm.Key = k
		elm.Value = v
		e = append(e, elm)
	}

	return e, nil
}

// Serialze turns a list into bytes of gob.
func (c *listHdlr) Serialize() ([]byte, error) {

	var data []byte
	var encoded bytes.Buffer

	items := c.GetMap()

	encode := gob.NewEncoder(&encoded)
	err := encode.Encode(items)
	if err != nil {
		return nil, err
	}

	s64based := base64.StdEncoding.EncodeToString(encoded.Bytes())
	data = []byte(s64based)

	return data, nil
}
func (c *listHdlr) DeserializeFromFile(fPath string) ([]Element, error) {

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

	e, err := c.Deserialize(data)
	if err != nil {
		return nil, err
	}

	return e, nil
}

func (c *listHdlr) SerializeToFile(fPath string) error {

	var data []byte

	items := c.GetMap()
	b, err := json.Marshal(items)
	if err != nil {
		return err
	}
	s64based := base64.StdEncoding.EncodeToString(b)
	data = []byte(s64based)

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
