package generate

import (
	"errors"
	"fmt"
	"github.com/gocql/gocql"
	"reflect"
	"time"
)

func cassaType(i interface{}) gocql.Type {
	switch i.(type) {
	case int, int32:
		return gocql.TypeInt
	case int64:
		return gocql.TypeBigInt
	case string:
		return gocql.TypeVarchar
	case float32:
		return gocql.TypeFloat
	case float64:
		return gocql.TypeDouble
	case bool:
		return gocql.TypeBoolean
	case time.Time:
		return gocql.TypeTimestamp
	case gocql.UUID:
		return gocql.TypeUUID
	case []byte:
		return gocql.TypeBlob
	}
	return gocql.TypeCustom
}

func stringTypeOf(i interface{}) (string, error) {
	_, isByteSlice := i.([]byte)
	if !isByteSlice {
		// Check if we found a higher kinded type
		switch reflect.ValueOf(i).Kind() {
		case reflect.Slice:
			elemVal := reflect.Indirect(reflect.New(reflect.TypeOf(i).Elem())).Interface()
			ct := cassaType(elemVal)
			if ct == gocql.TypeCustom {
				return "", fmt.Errorf("Unsupported type %T", i)
			}
			return fmt.Sprintf("list<%v>", ct), nil
		case reflect.Map:
			keyVal :=reflect.Indirect(reflect.New(reflect.TypeOf(i).Key())).Interface()
			elemVal := reflect.Indirect(reflect.New(reflect.TypeOf(i).Elem())).Interface()
			keyCt := cassaType(keyVal)
			elemCt := cassaType(elemVal)
			if keyCt == gocql.TypeCustom || elemCt == gocql.TypeCustom {
				return "", fmt.Errorf("Unsupported map key or value type %T", i)
			}
			return fmt.Sprintf("map<%v, %v>", keyCt, elemCt), nil
		}
	}
	ct := cassaType(i)
	if ct == gocql.TypeCustom {
		return "", fmt.Errorf("Unsupported type %T", i)
	}
	return cassaTypeToString(ct)
}

func cassaTypeToString(t gocql.Type) (string, error) {
	switch t {
	case gocql.TypeInt:
		return "int", nil
	case gocql.TypeBigInt:
		return "bigint", nil
	case gocql.TypeVarchar:
		return "varchar", nil
	case gocql.TypeFloat:
		return "float", nil
	case gocql.TypeDouble:
		return "double", nil
	case gocql.TypeBoolean:
		return "boolean", nil
	case gocql.TypeTimestamp:
		return "timestamp", nil
	case gocql.TypeUUID:
		return "uuid", nil
	case gocql.TypeBlob:
		return "blob", nil
	default:
		return "", errors.New("unkown cassandra type")
	}
}
