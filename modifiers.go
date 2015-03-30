package gocassa

import (
	"bytes"
	"fmt"
	"strings"
)

// Modifiers are used with update statements.

const (
	modifierListPrepend = iota
	modifierListAppend
	modifierListSetAtIndex
	modifierListRemove
	modifierListRemoveAtIndex
	modifierMapSetFields
	modifierMapSetField
	modifierMapReplace
	modifierMapDeleteField
	modifierCounterIncrement
)

type Modifier struct {
	op   int
	args []interface{}
}

// ListPrepend prepends a value to the front of the list
func ListPrepend(value interface{}) Modifier {
	return Modifier{
		op:   modifierListPrepend,
		args: []interface{}{value},
	}
}

// ListAppend appends a value to the end of the list
func ListAppend(value interface{}) Modifier {
	return Modifier{
		op:   modifierListAppend,
		args: []interface{}{value},
	}
}

// ListSetAtIndex sets the list element at a given index to a given value
func ListSetAtIndex(index int, value interface{}) Modifier {
	return Modifier{
		op:   modifierListSetAtIndex,
		args: []interface{}{index, value},
	}
}

// ListRemove removes all elements from a list having a particular value
func ListRemove(value interface{}) Modifier {
	return Modifier{
		op:   modifierListRemove,
		args: []interface{}{value},
	}
}

// This uses DELETE, not UPDATE
// Removes an element at a specific index from the list
// func ListRemoveAtIndex(index int, value interface{}) Modifier {
// 	return Modifier{
// 		op:   ModifierListRemove,
// 		args: []interface{}{index, value},
// 	}
// }

// MapSetFields updates the map with keys and values in the given map
func MapSetFields(fields map[string]interface{}) Modifier {
	return Modifier{
		op:   modifierMapSetFields,
		args: []interface{}{fields},
	}
}

// @todo MapReplace
// @todo MapDelete

// MapSetField updates the map with the given key and value
func MapSetField(key, value interface{}) Modifier {
	return Modifier{
		op:   modifierMapSetField,
		args: []interface{}{key, value},
	}
}

// CounterIncrement increments the value of the counter with the given value.
// Negative value results in decrementing.
func CounterIncrement(value int) Modifier {
	return Modifier{
		op:   modifierCounterIncrement,
		args: []interface{}{value},
	}
}

func (m Modifier) cql(name string) (string, []interface{}) {
	str := ""
	vals := []interface{}{}
	switch m.op {
	// Can not use bind variables here due to "bind variables are not supported inside collection literals" :(
	case modifierListPrepend:
		str = fmt.Sprintf("%v = [%v] + %v", name, printElem(m.args[0]), name)
	case modifierListAppend:
		str = fmt.Sprintf("%v = %v + [%v]", name, name, printElem(m.args[0]))
	case modifierListSetAtIndex:
		str = fmt.Sprintf("%v[%v] = %v", name, m.args[0], printElem(m.args[1]))
	case modifierListRemove:
		str = fmt.Sprintf("%v = %v - [%v]", name, name, printElem(m.args[0]))
	case modifierMapSetFields:
		buf := new(bytes.Buffer)
		buf.WriteString(fmt.Sprintf("%v = %v + ", name, name))
		ma, ok := m.args[0].(map[string]interface{})
		if !ok {
			panic(fmt.Sprintf("Argument for MapSetFields is not a map: %v", m.args[0]))
		}
		buf.WriteString("{")
		i := 0
		for k, v := range ma {
			if i > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(fmt.Sprintf("%v : %v", printElem(k), printElem(v)))
			i++
		}
		buf.WriteString("}")
		str = buf.String()
	case modifierMapSetField:
		str = fmt.Sprintf("%v[%v] = %v", name, printElem(m.args[0]), printElem(m.args[1]))
	case modifierCounterIncrement:
		val := m.args[0].(int)
		if val > 0 {
			str = fmt.Sprintf("%v = %v + %v", name, name, printElem(val))
		} else {
			str = fmt.Sprintf("%v = %v - %v", name, name, printElem(val*-1))
		}
	}
	return str, vals
}

func printElem(i interface{}) string {
	switch v := i.(type) {
	case string:
		return "'" + escape(v) + "'"
	}
	return fmt.Sprintf("%v", i)
}

func escape(s string) string {
	return strings.Replace(s, "'", "''", -1)
}
