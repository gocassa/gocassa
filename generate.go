package gocassa

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/gocql/gocql"
)

// CREATE TABLE users (
//   user_name varchar PRIMARY KEY,
//   password varchar,
//   gender varchar,
//   session_token varchar,
//   state varchar,
//   birth_year bigint
// );
//
// CREATE TABLE emp (
//   empID int,
//   deptID int,
//   first_name varchar,
//   last_name varchar,
//   PRIMARY KEY (empID, deptID)
// );
//

func createTable(keySpace, cf string, partitionKeys, colKeys []string, fields []string, values []interface{}, order []ClusteringOrderColumn, compoundKey bool) (string, error) {
	firstLine := fmt.Sprintf("CREATE TABLE %v.%v (", keySpace, cf)
	fieldLines := []string{}
	for i, _ := range fields {
		typeStr, err := stringTypeOf(values[i])
		if err != nil {
			return "", err
		}
		l := "    " + strings.ToLower(fields[i]) + " " + typeStr
		fieldLines = append(fieldLines, l)
	}
	//key generation
	str := ""
	if len(colKeys) > 0 { //key (or composite key) + clustering columns
		str = "    PRIMARY KEY ((%v), %v)"
	} else if compoundKey { //compound key just one set of parenthesis
		str = "    PRIMARY KEY (%v %v)"
	} else { //otherwise is a composite key without colKeys
		str = "    PRIMARY KEY ((%v %v))"
	}

	fieldLines = append(fieldLines, fmt.Sprintf(str, j(partitionKeys), j(colKeys)))

	lines := []string{
		firstLine,
		strings.Join(fieldLines, ",\n"),
		")",
	}

	if len(order) > 0 {
		orderStrs := make([]string, len(order))
		for i, o := range order {
			dir := "ASC"
			if o.Direction == DESC {
				dir = "DESC"
			}
			orderStrs[i] = fmt.Sprintf("%v %v", o.Column, dir)
		}
		orderLine := fmt.Sprintf("WITH CLUSTERING ORDER BY (%v)", strings.Join(orderStrs, ", "))
		lines = append(lines, orderLine)
	}

	lines = append(lines, ";")
	stmt := strings.Join(lines, "\n")
	return stmt, nil
}

func j(s []string) string {
	s1 := []string{}
	for _, v := range s {
		s1 = append(s1, strings.ToLower(v))
	}
	return strings.Join(s1, ", ")
}

func createKeyspace(keyspaceName string) string {
	return fmt.Sprintf("CREATE KEYSPACE \"%v\" WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'eu-west-1' : 3}", keyspaceName)
}

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
	case Counter:
		return gocql.TypeCounter
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
			keyVal := reflect.Indirect(reflect.New(reflect.TypeOf(i).Key())).Interface()
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
	case gocql.TypeCounter:
		return "counter", nil
	default:
		return "", errors.New("unkown cassandra type")
	}
}
