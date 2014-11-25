// This package generates all kind of CQL statements
package generate

import (
	"errors"
	"fmt"
	"github.com/gocql/gocql"
	"strings"
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
//
// To be done: main difficulty here is there is no mapping between go types and cassa types in gocql, it does a cassa
// inflight to get the table meat (AFAIK)
func CreateTable(cfName, primaryKey string, fields []string, values []interface{}) (string, error) {
	// for _, v := range primaryKey {
	// 	if _, ok := m[v]; !ok {
	// 		return "", errors.New("missing primary key " + v)
	// 	}
	// }
	firstLine := fmt.Sprintf("CREATE TABLE %v (", cfName)
	fieldLines := []string{}
	for i, v := range fields {
		ct := cassaType(values[i])
		if ct == gocql.TypeCustom {
			return "", errors.New(fmt.Sprintf("Unsupported type %T", values[i]))
		}
		typ, err := cassaTypeToString(ct)
		if err != nil {
			return "", nil
		}
		l := "    " + fields[i] + " " + typ
		if v == primaryKey {
			l += " PRIMARY KEY"
		}
		fieldLines = append(fieldLines, l)
	}
	stmt := strings.Join([]string{firstLine, strings.Join(fieldLines, ",\n"), ");"}, "\n")
	return stmt, nil
}

func CreateKeyspace(keyspaceName string) string {
	// This must come from the go-service layer
	return fmt.Sprintf("CREATE KEYSPACE \"%v\" WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'eu-west-1' : 3}", keyspaceName)
}

// UPDATE Movies SET col1 = val1, col2 = val2 WHERE movieID = key1;
func UpdateById(cfName string, pkName string, fieldNames []string) string {
	cols := []string{}
	for _, v := range fieldNames {
		cols = append(cols, v+" = ?")
	}
	return fmt.Sprintf("UPDATE %v SET "+strings.Join(cols, ", ")+" WHERE %v = ?;", cfName, pkName)
}

// INSERT INTO Hollywood.NerdMovies (user_uuid, fan)
//   VALUES ('cfd66ccc-d857-4e90-b1e5-df98a3d40cd6', 'johndoe')
//
// Gotcha: primkey must be first
func Insert(cfName string, fieldNames []string) string {
	placeHolders := []string{}
	for i := 0; i < len(fieldNames); i++ {
		placeHolders = append(placeHolders, "?")
	}
	return fmt.Sprintf("INSERT INTO %v ("+strings.Join(fieldNames, ", ")+") VALUES ("+strings.Join(placeHolders, ", ")+")", cfName)
}

func ReadById(cfName string, pk string) string {
	return fmt.Sprintf("SELECT * FROM %v WHERE %v = ?", cfName, pk)
}

// DELETE email, phone
//  FROM users
//  USING CONSISTENCY QUORUM AND TIMESTAMP 1318452291034
//  WHERE user_name = 'jsmith';
func DeleteById(cfName, pkName string) string {
	return fmt.Sprintf("DELETE FROM %v WHERE %v = ?;", cfName, pkName)
}

func UseKeyspace(keyspaceName string) string {
	return fmt.Sprintf("USE \"%v\";", keyspaceName)
}