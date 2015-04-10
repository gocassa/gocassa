package gocassa

type filter struct {
	t  t
	rs []Relation
}

func (f filter) Update(m map[string]interface{}) Op {
	return newWriteOp(f.t.keySpace.qe, f, update, m)
}

func (f filter) Delete() Op {
	return newWriteOp(f.t.keySpace.qe, f, delete, nil)
}

//
// Reads
//

func (f filter) Read(pointerToASlice interface{}) Op {
	return &op{
		qe: f.t.keySpace.qe,
		ops: []singleOp{
			{
				f:      f,
				opType: read,
				result: pointerToASlice,
			},
		},
	}
}

func (f filter) ReadOne(pointer interface{}) Op {
	return &op{
		qe: f.t.keySpace.qe,
		ops: []singleOp{
			{
				f:      f,
				opType: singleRead,
				result: pointer,
			},
		},
	}
}
