package gocassa

import (
)

type oneToOneT struct {
	*t
	idField string
}

func (o *oneToOneT) Update(id interface{}, m map[string]interface{}) WriteOp {
	return o.Where(Eq(o.idField, id)).Update(m)
}

func (o *oneToOneT) UpdateWithOptions(id interface{}, m map[string]interface{}, opts Options) WriteOp {
	return o.Where(Eq(o.idField, id)).UpdateWithOptions(m, opts)
}

func (o *oneToOneT) Delete(id interface{}) WriteOp {
	return o.Where(Eq(o.idField, id)).Delete()
}

func (o *oneToOneT) Read(id, pointer interface{}) error {
	return o.Where(Eq(o.idField, id)).Query().ReadOne(pointer)
}

func (o *oneToOneT) MultiRead(ids []interface{}, pointerToASlice interface{}) error {
	return o.Where(In(o.idField, ids...)).Query().Read(pointerToASlice)
}
