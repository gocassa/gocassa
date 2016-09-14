package gocassa

import (
	"bytes"
	"fmt"
	"strings"
	"time"
)

// Modifiers are used with update statements.

const (
	modifierListPrepend = iota
	modifierListAppend
	modifierListSetAtIndex
	modifierListRemove
	modifierMapSetFields
	modifierMapSetField
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

// MapSetFields updates the map with keys and values in the given map
func MapSetFields(fields map[string]interface{}) Modifier {
	return Modifier{
		op:   modifierMapSetFields,
		args: []interface{}{fields},
	}
}

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
	case modifierListPrepend:
		str = fmt.Sprintf("%s = ? + %s", name, name)
		vals = append(vals, []interface{}{m.args[0]})
	case modifierListAppend:
		str = fmt.Sprintf("%s = %s + ?", name, name)
		vals = append(vals, []interface{}{m.args[0]})
	case modifierListSetAtIndex:
		str = fmt.Sprintf("%s[?] = ?", name)
		vals = append(vals, m.args[0], m.args[1])
	case modifierListRemove:
		str = fmt.Sprintf("%s = %s - ?", name)
		vals = append(vals, m.args[0])
	case modifierMapSetFields:
		fields, ok := m.args[0].(map[string]interface{})
		if !ok {
			panic(fmt.Sprintf("Argument for MapSetFields is not a map: %v", m.args[0]))
		}

		buf := new(bytes.Buffer)
		i := 0
		for k, v := range fields {
			if i > 0 {
				buf.WriteString(", ")
			}

			fieldStmt, fieldVals := MapSetField(k, v).cql(name)
			buf.WriteString(fieldStmt)
			vals = append(vals, fieldVals...)

			i++
		}
		str = buf.String()
	case modifierMapSetField:
		str = fmt.Sprintf("%s[?] = ?", name)
		vals = append(vals, m.args[0], m.args[1])
	case modifierCounterIncrement:
		val := m.args[0].(int)
		if val > 0 {
			str = fmt.Sprintf("%s = %s + ?", name, name)
			vals = append(vals, val)
		} else {
			str = fmt.Sprintf("%s = %s - ?", name, name)
			vals = append(vals, -val)
		}
	}
	return str, vals
}

func printElem(i interface{}) string {
	switch v := i.(type) {
	case string:
		return "'" + escape(v) + "'"
	case time.Time:
		return "'" + escape(fmt.Sprintf("%v", unixMilli(i.(time.Time)))) + "'"
	}
	return fmt.Sprintf("%v", i)
}

// unixMilli returns the number of milliseconds since epoch
func unixMilli(t time.Time) int64 {
	return t.Unix()*1e3 + int64(t.Nanosecond()/1e6)
}

func escape(s string) string {
	return strings.Replace(s, "'", "''", -1)
}
