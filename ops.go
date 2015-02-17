package gocassa

import(
	"encoding/json"
	"fmt"
)

const(
	write = iota
	read
	singleRead
)

type op struct {
	opType int
	result interface{} // 
	qe QueryExecutor 
	stmt string 
	params []interface{}
}

func newWriteOp(qe QueryExecutor, stmt string, params []interface{}) *op {
	return &op{
		opType: write,
		qe: qe,
		stmt: stmt,
		params: params,
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
		return fmt.Errorf("Can not read one: no results")
	}
	bytes, err := json.Marshal(maps[0])
	if err != nil {
		return err
	}
	return json.Unmarshal(bytes, w.result)
}

func (w *op) Run() error {
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

func (w *op) Add(wo ...Op) Ops {
	return ops{
		ws: append([]Op{Op(w)}, wo...),
	}
}

type ops struct {
	qe QueryExecutor
	ws []Op
}

func (w ops) Run() error {
	for _, v := range w.ws {
		err := v.Run()
		if err != nil {
			return err
		}
	}
	return nil
}

func (w ops) RunAtomically() error {
	stmts := []string{}
	params := [][]interface{}{}
	for _, v := range w.ws {
		// We are pushing the limits of the type system here...
		vop := v.(*op)
		if vop.opType == write {
			stmts = append(stmts, vop.stmt)
			params = append(params, vop.params)
		}
	}
	return w.qe.ExecuteAtomically(stmts, params)
}

func (w ops) Add(wo ...Op) Ops {
	return ops{
		qe: w.qe,
		ws: append(w.ws, wo...),
	}
}