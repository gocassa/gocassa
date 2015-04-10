package gocassa

import (
	"fmt"
	"strings"
)

// RowNotFoundError is returned by Reads if the Row is not found.
type RowNotFoundError struct {
	file string
	line int
}

func (r RowNotFoundError) Error() string {
	ss := strings.Split(r.file, "/")
	f := ""
	if len(ss) > 0 {
		f = ss[len(ss)-1]
	}
	return fmt.Sprintf("%v:%v: No rows returned", f, r.line)
}
