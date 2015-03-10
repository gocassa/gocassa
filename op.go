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
	err error
}

func (op *op) Error() string {
	if op.err != nil {
		return op.err.Error()
	}
	return ""
}

type singleOp struct {
	opType int
	result interface{}
	stmt   string
	params []interface{}
}

func newWriteOp(qe QueryExecutor, stmt string, params []interface{}, isBatch bool) *op {
	return newOp(qe, write, nil, stmt, params, isBatch)
}

func newOp(qe QueryExecutor, opType int, result interface{}, stmt string, params []interface{}, isBatch bool) *op {
	o := &op{
		qe: qe,
		ops: []singleOp{
			{
				opType: opType,
				result: result,
				stmt:   stmt,
				params: params,
			},
		},
	}
	if isBatch {
		return o
	}

	err := o.runAll()
	if err == nil {
		return nil
	}

	return &op{
		err: err,
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

func (w *op) add(wo ...Op) Op {
	for _, v := range wo {
		w.ops = append(w.ops, v.(*op).ops...)
	}
	return w
}

func (w op) runAll() error {
	for _, v := range w.ops {
		err := v.run(w.qe)
		if err != nil {
			return err
		}
	}
	return nil
}
