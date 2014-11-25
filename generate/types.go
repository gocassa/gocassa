package generate

import (
	"errors"
	"github.com/gocql/gocql"
)

func cassaType(i interface{}) gocql.Type {
	switch i.(type) {
	case int:
		return gocql.TypeInt
	case string:
		return gocql.TypeVarchar
	case float32:
		return gocql.TypeFloat
	case float64:
		return gocql.TypeDouble
	case bool:
		return gocql.TypeBoolean
	}
	return gocql.TypeCustom
}

func cassaTypeToString(t gocql.Type) (string, error) {
	switch t {
	case gocql.TypeInt:
		return "int", nil
	case gocql.TypeVarchar:
		return "varchar", nil
	case gocql.TypeFloat:
		return "float", nil
	case gocql.TypeDouble:
		return "double", nil
	case gocql.TypeBoolean:
		return "boolean", nil
	}
	return "", errors.New("om.cassaTypeToString: unkown type")
}