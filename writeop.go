package gocassa

type writeOp struct {
	qe QueryExecutor 
	stmt string 
	params []interface{}
}

func newWriteOp(qe QueryExecutor, stmt string, params []interface{}) *writeOp {
	return &writeOp{
		qe: qe,
		stmt: stmt,
		params: params,
	}
}

func (w *writeOp) Run() error {
	return w.qe.Execute(w.stmt, w.params...)
}

func (w *writeOp) Add(wo ...WriteOp) WriteOps {
	return writeOps{
		ws: append([]WriteOp{WriteOp(w)}, wo...),
	}
}

type writeOps struct {
	qe QueryExecutor
	ws []WriteOp
}

func (w writeOps) Run() error {
	for _, v := range w.ws {
		err := v.Run()
		if err != nil {
			return err
		}
	}
	return nil
}

func (w writeOps) RunBatched() error {
	stmts := []string{}
	params := [][]interface{}{}
	for _, v := range w.ws {
		// We are pushing the limits of the type system here...
		stmts = append(stmts, v.(*writeOp).stmt)
		params = append(params, v.(*writeOp).params)
	}
	return w.qe.ExecuteBatched(stmts, params)
}

func (w writeOps) Add(wo ...WriteOp) WriteOps {
	return writeOps {
		qe: w.qe,
		ws: append(w.ws, wo...),
	}
}