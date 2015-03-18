package gocassa

import (
	"fmt"
	"strings"
)

// RowNotFoundError is returned by Reads if the Row is not found.
type RowNotFoundError struct {
	stmt   string
	params []interface{}
}

func (r RowNotFoundError) Error() string {
	// This is not optimal at all
	completCql := fmt.Sprintf(strings.Replace(r.stmt, "?", "%v", -1), r.params...)
	return "The following query returned no results: " + completCql
}
