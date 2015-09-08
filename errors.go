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

// errOp is an Op which represents a known error, which will always return during preflighting (preventing any execution
// in a multiOp scenario)
type errOp struct{ err error }

func (o errOp) Run() error                                 { return o.err }
func (o errOp) RunAtomically() error                       { return o.err }
func (o errOp) Add(ops ...Op) Op                           { return multiOp{o}.Add(ops...) }
func (o errOp) WithOptions(_ Options) Op                   { return o }
func (o errOp) Preflight() error                           { return o.err }
func (o errOp) GenerateStatement() (string, []interface{}) { return "", []interface{}{} }
func (o errOp) QueryExecutor() QueryExecutor               { return nil }
