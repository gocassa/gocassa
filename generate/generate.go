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
func CreateTable(keySpace, cf string, partitionKeys, colKeys []string, fields []string, values []interface{}) (string, error) {
	firstLine := fmt.Sprintf("CREATE TABLE %v.%v (", keySpace, cf)
	fieldLines := []string{}
	for i := range fields {
		ct := cassaType(values[i])
		if ct == gocql.TypeCustom {
			return "", errors.New(fmt.Sprintf("Unsupported type %T", values[i]))
		}
		typ, err := cassaTypeToString(ct)
		if err != nil {
			return "", nil
		}
		l := "    " + fields[i] + " " + typ
		fieldLines = append(fieldLines, l)
	}
	str := "    PRIMARY KEY ((%v) %v)"
	if len(colKeys) > 0 {
		str = "    PRIMARY KEY ((%v), %v)"
	}
	fieldLines = append(fieldLines, fmt.Sprintf(str, j(partitionKeys), j(colKeys)))
	stmt := strings.Join([]string{firstLine, strings.Join(fieldLines, ",\n"), ");"}, "\n")
	return stmt, nil
}

func j(s []string) string {
	return strings.Join(s, ", ")
}

func CreateKeyspace(keyspaceName string) string {
	// This must come from the go-service layer
	return fmt.Sprintf("CREATE KEYSPACE \"%v\" WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'eu-west-1' : 3}", keyspaceName)
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
