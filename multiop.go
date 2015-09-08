package gocassa

type multiOp []Op

func Noop() Op {
	return multiOp(nil)
}

func (mo multiOp) Run() error {
	if err := mo.Preflight(); err != nil {
		return err
	}
	for _, op := range mo {
		if err := op.Run(); err != nil {
			return err
		}
	}
	return nil
}

func (mo multiOp) RunAtomically() error {
	if err := mo.Preflight(); err != nil {
		return err
	}
	stmts := make([]string, len(mo))
	vals := make([][]interface{}, len(mo))
	var qe QueryExecutor
	for i, op := range mo {
		s, v := op.GenerateStatement()
		qe = op.QueryExecutor()
		stmts[i] = s
		vals[i] = v
	}

	return qe.ExecuteAtomically(stmts, vals)
}

func (mo multiOp) GenerateStatement() (string, []interface{}) {
	return "", []interface{}{}
}

func (mo multiOp) QueryExecutor() QueryExecutor {
	if len(mo) == 0 {
		return nil
	}
	return mo[0].QueryExecutor()
}

func (mo multiOp) Add(ops_ ...Op) Op {
	ops := make([]Op, 0, len(ops_))
	for _, op := range ops_ {
		// If any multiOps were passed, flatten them out
		switch op := op.(type) {
		case multiOp:
			ops = append(ops, op...)
		default:
			ops = append(ops, op)
		}
	}
	return append(mo, ops...)
}

func (mo multiOp) WithOptions(opts Options) Op {
	result := make(multiOp, len(mo))
	for i, op := range mo {
		result[i] = op.WithOptions(opts)
	}
	return result
}

func (mo multiOp) Preflight() error {
	for _, op := range mo {
		if err := op.Preflight(); err != nil {
			return err
		}
	}
	return nil
}
