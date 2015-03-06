package gocassa

import (
	"fmt"
	"strings"
)

// Modifiers are used with update statements.

const (
	ModifierListPrepend = iota
	ModifierListAppend
	ModifierListSetAtIndex
	ModifierListRemove
	// ModifierListRemoveAtIndex
)

type Modifier struct {
	op   int
	args []interface{}
}

// Prepend a value to the front of the list
func ListPrepend(value interface{}) Modifier {
	return Modifier{
		op:   ModifierListPrepend,
		args: []interface{}{value},
	}
}

// Append a value to the end of the list
func ListAppend(value interface{}) Modifier {
	return Modifier{
		op:   ModifierListAppend,
		args: []interface{}{value},
	}
}

// Sets list element at index to value
func ListSetAtIndex(index int, value interface{}) Modifier {
	return Modifier{
		op:   ModifierListSetAtIndex,
		args: []interface{}{index, value},
	}
}

// Remove all elements having a particular value
func ListRemove(value interface{}) Modifier {
	return Modifier{
		op:   ModifierListRemove,
		args: []interface{}{value},
	}
}

// Removes an element at a specific index from the list
// func ListRemoveAtIndex(index int, value interface{}) Modifier {
// 	return Modifier{
// 		op:   ModifierListRemove,
// 		args: []interface{}{index, value},
// 	}
// }

// returns a string with a %v placeholder for field name
func (m Modifier) cql(name string) (string, []interface{}) {
	str := ""
	vals := []interface{}{}
	switch m.op {
	// Can not use bind variables here due to "bind variables are not supported inside collection literals" :(
	case ModifierListPrepend:
		str = fmt.Sprintf("%v = [%v] + %v", name, printElem(m.args[0]), name)
	case ModifierListAppend:
		str = fmt.Sprintf("%v = %v + [%v]", name, name, printElem(m.args[0]))
	case ModifierListSetAtIndex:
		str = fmt.Sprintf("%v[%v] = %v", name, m.args[0], printElem(m.args[1]))
	case ModifierListRemove:
		str = fmt.Sprintf("%v = %v - [%v]", name, name, printElem(m.args[0]))
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
