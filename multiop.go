package gocassa

import (
	"errors"
)

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
	return errors.New("RunAtomically() is not implemented yet")
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
