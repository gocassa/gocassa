package gocassa

import "reflect"

type statement struct {
	fieldNames []string
	fieldTypes []reflect.Type

	values []interface{}
	query  string
}

var noOpStatement = newStatement("", []interface{}{})

// FieldNames contains the column names which will be selected
// This will only be populated for SELECT queries
func (s statement) FieldNames() []string {
	return s.fieldNames
}

// FieldTypes contains the binding types of columns selected
// in their referenced struct. This will only be populated for
// SELECT queries
func (s statement) FieldTypes() []reflect.Type {
	return s.fieldTypes
}

// Values encapsulates binding values to be set within the CQL
// query string as binding parameters. If there are no binding
// parameters in the query, this will be the empty slice
func (s statement) Values() []interface{} {
	return s.values
}

// Query returns the CQL query for this statement
func (s statement) Query() string {
	return s.query
}

func newStatement(query string, values []interface{}) statement {
	return statement{
		query:  query,
		values: values,
	}
}

func newSelectStatement(query string, values []interface{}, fieldNames []string, fieldTypes []reflect.Type) statement {
	return statement{
		query:      query,
		values:     values,
		fieldNames: fieldNames,
		fieldTypes: fieldTypes,
	}
}
