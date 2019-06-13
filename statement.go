package gocassa

type statement struct {
	columns []string
	values  []interface{}
	query   string
}

var noOpStatement = newStatement("", []interface{}{})

// ColumnNames contains the column names which will be selected
// This will only be populated for SELECT queries
func (s statement) ColumnNames() []string {
	return s.columns
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

func newSelectStatement(query string, values []interface{}, columnNames []string) statement {
	return statement{
		query:   query,
		values:  values,
		columns: columnNames,
	}
}
