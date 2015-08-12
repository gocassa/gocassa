package gocassa

type filter struct {
	t  t
	rs []Relation
}

func (f filter) Update(m map[string]interface{}) Op {
	return newWriteOp(f.t.keySpace.qe, f, updateOpType, m)
}

func (f filter) Delete() Op {
	return newWriteOp(f.t.keySpace.qe, f, deleteOpType, nil)
}

//
// Reads
//

func (f filter) Read(pointerToASlice interface{}) Op {
	return &singleOp{
		qe:     f.t.keySpace.qe,
		f:      f,
		opType: readOpType,
		result: pointerToASlice}
}

func (f filter) ReadOne(pointer interface{}) Op {
	return &singleOp{
		qe:     f.t.keySpace.qe,
		f:      f,
		opType: singleReadOpType,
		result: pointer}
}
