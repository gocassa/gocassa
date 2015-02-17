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
	qe  QueryExecutor
	ops []singleOp
}

type singleOp struct {
	opType int
	result interface{}
	stmt   string
	params []interface{}
}

func newWriteOp(qe QueryExecutor, stmt string, params []interface{}) *op {
	return &op{
		qe: qe,
		ops: []singleOp{
			{
				opType: write,
				stmt:   stmt,
				params: params,
			},
		},
	}
}

func (w *singleOp) read(qe QueryExecutor) error {
	maps, err := qe.Query(w.stmt, w.params...)
	if err != nil {
		return err
	}
	bytes, err := json.Marshal(maps)
	if err != nil {
		return err
	}
	return json.Unmarshal(bytes, w.result)
}

func (w *singleOp) readOne(qe QueryExecutor) error {
	maps, err := qe.Query(w.stmt, w.params...)
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

func (w *singleOp) run(qe QueryExecutor) error {
	switch w.opType {
	case write:
		return qe.Execute(w.stmt, w.params...)
	case read:
		return w.read(qe)
	case singleRead:
		return w.readOne(qe)
	}
	return nil
}

func (w *op) Add(wo ...Op) Op {
	for _, v := range wo {
		w.ops = append(w.ops, v.(*op).ops...)
	}
	return w
}

func (w op) Run() error {
	for _, v := range w.ops {
		err := v.run(w.qe)
		if err != nil {
			return err
		}
	}
	return nil
}

func (w op) RunAtomically() error {
	stmts := []string{}
	params := [][]interface{}{}
	for _, vop := range w.ops {
		// We are pushing the limits of the type system here...
		if vop.opType == write {
			stmts = append(stmts, vop.stmt)
			params = append(params, vop.params)
		}
	}
	return w.qe.ExecuteAtomically(stmts, params)
}
