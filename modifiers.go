package gocassa

import (
	"fmt"
	"strings"
	"bytes"
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
	modifierCounterAdd
)

type Modifier struct {
	op   int
	args []interface{}
}

// Prepend a value to the front of the list
func ListPrepend(value interface{}) Modifier {
	return Modifier{
		op:   modifierListPrepend,
		args: []interface{}{value},
	}
}

// Append a value to the end of the list
func ListAppend(value interface{}) Modifier {
	return Modifier{
		op:   modifierListAppend,
		args: []interface{}{value},
	}
}

// Sets list element at index to value
func ListSetAtIndex(index int, value interface{}) Modifier {
	return Modifier{
		op:   modifierListSetAtIndex,
		args: []interface{}{index, value},
	}
}

// Remove all elements having a particular value
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

func MapSetFields(fields map[string]interface{}) Modifier {
	return Modifier{
		op:   modifierMapSetFields,
		args: []interface{}{fields},
	}
}

// @todo MapReplace
// @todo MapDelete

func MapSetField(key, value interface{}) Modifier {
	return Modifier{
		op:   modifierMapSetField,
		args: []interface{}{key, value},
	}
}

func CounterAdd(value int) Modifier {
	return Modifier{
		op:   modifierCounterAdd,
		args: []interface{}{value},
	}
}

// returns a string with a %v placeholder for field name
func (m Modifier) cql(name string) (string,  []interface{}) {
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
		buf.WriteString(fmt.Sprintf("%v = ", name))
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
	case modifierCounterAdd:
		str = fmt.Sprintf("%v = %v + %v", name, name, printElem(m.args[0]))
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
