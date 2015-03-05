package gocassa

// Modifiers are used with update statements.

const (
	ModifierListPrepend = iota
	ModifierListAppend
	//ModifierListSetAtIndex
	ModifierListRemove
)

type Modifier struct {
	op 		int
	args 	[]interface{}
}

func ListPrepend(value interface{}) Modifier {
	return Modifier{
		op: ModifierListPrepend,
		args: []interface{}{value},
	}
}

func ListAppend(value interface{}) Modifier {
	return Modifier{
		op: ModifierListAppend,
		args: []interface{}{value},
	}
}

// Leaving this uncommented for the time being
// func ListSetAtIndex(index int, value interface{}) Modifier {
// 	return Modifier{
// 		op: ModifierListAppend,
// 		args: []interface{}{value},
// 	}
// }

func ListRemove(value interface{}) Modifier {
	return Modifier{
		op: ModifierListRemove,
		args: []interface{}{value},
	}
}

// returns a string with a %v placeholder for field name
func (m Modifier) cql() (string, []interface{}) {
	switch m.op {
	case ModifierListPrepend:
		return "[?] + %v", m.args
	case ModifierListAppend:
		return "%v + [?]", m.args
	//case ModifierListSetAtIndex:
	case ModifierListRemove:
		return "%v - [?]", m.args
	}
	return "", []interface{}{}
}
