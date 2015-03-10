package gocassa

import (
	"encoding/json"
)

const (
	write = iota
	read
	singleRead
)

type op struct {
	qe     QueryExecutor
	opType int
	result interface{}
	stmt   string
	params []interface{}
	err    error
}

func (op *op) Error() string {
	if op.err != nil {
		return op.err.Error()
	}
	return ""
}

func newWriteOp(qe QueryExecutor, stmt string, params []interface{}, isBatch bool) *op {
	return newOp(qe, write, nil, stmt, params, isBatch)
}

func newOp(qe QueryExecutor, opType int, result interface{}, stmt string, params []interface{}, isBatch bool) *op {
	o := &op{
		qe:     qe,
		opType: opType,
		result: result,
		stmt:   stmt,
		params: params,
	}

	if isBatch {
		return o
	}

	err := o.run()
	if err == nil {
		return nil
	}

	return &op{
		err: err,
	}
}

func (w *op) read() error {
	maps, err := w.qe.Query(w.stmt, w.params...)
	if err != nil {
		return err
	}
	bytes, err := json.Marshal(maps)
	if err != nil {
		return err
	}
	return json.Unmarshal(bytes, w.result)
}

func (w *op) readOne() error {
	maps, err := w.qe.Query(w.stmt, w.params...)
	if err != nil {
		return err
	}
	if len(maps) == 0 {
		return RowNotFoundError{
			stmt:   w.stmt,
			params: w.params,
		}
	}
	bytes, err := json.Marshal(maps[0])
	if err != nil {
		return err
	}
	return json.Unmarshal(bytes, w.result)
}

func (w *op) run() error {
	switch w.opType {
	case write:
		return w.qe.Execute(w.stmt, w.params...)
	case read:
		return w.read()
	case singleRead:
		return w.readOne()
	}
	return nil
}

func RunAtomically(ops ...Op) error {
	stmts := []string{}
	params := [][]interface{}{}
	var qe QueryExecutor

	for _, o := range ops {
		vop := o.(*op)
		qe = vop.qe
		if vop.opType == write {
			stmts = append(stmts, vop.stmt)
			params = append(params, vop.params)
		}
	}

	return qe.ExecuteAtomically(stmts, params)
}
