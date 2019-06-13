package gocassa

import "context"

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

func (mo multiOp) RunWithContext(ctx context.Context) error {
	return mo.WithOptions(Options{Context: ctx}).Run()
}

func (mo multiOp) RunAtomically() error {
	if len(mo) == 0 {
		return nil
	}

	if err := mo.Preflight(); err != nil {
		return err
	}
	stmts := make([]Statement, len(mo))
	var qe QueryExecutor
	for i, op := range mo {
		s := op.GenerateStatement()
		qe = op.QueryExecutor()
		stmts[i] = s
	}

	return qe.ExecuteAtomicallyWithOptions(mo.Options(), stmts)
}

func (mo multiOp) RunAtomicallyWithContext(ctx context.Context) error {
	return mo.WithOptions(Options{Context: ctx}).RunAtomically()
}

func (mo multiOp) GenerateStatement() Statement {
	return noOpStatement
}

func (mo multiOp) QueryExecutor() QueryExecutor {
	if len(mo) == 0 {
		return nil
	}
	return mo[0].QueryExecutor()
}

func (mo multiOp) Add(ops_ ...Op) Op {
	if len(ops_) == 0 {
		return mo
	} else if len(mo) == 0 {
		switch len(ops_) {
		case 1:
			return ops_[0]
		default:
			return ops_[0].Add(ops_[1:]...)
		}
	}

	for _, op := range ops_ {
		// If any multiOps were passed, flatten them out
		switch op := op.(type) {
		case multiOp:
			mo = append(mo, op...)
		default:
			mo = append(mo, op)
		}
	}
	return mo
}

func (mo multiOp) Options() Options {
	var opts Options
	for _, op := range mo {
		opts = opts.Merge(op.Options())
	}
	return opts
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
