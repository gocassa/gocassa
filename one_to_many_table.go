package gocassa

import (

)

type oneToManyT struct {
	Table
	fieldToIndexBy string
	idField        string
}

func (o *oneToManyT) Update(field, id interface{}, m map[string]interface{}) error {
	return o.Where(Eq(o.fieldToIndexBy, field), Eq(o.idField, id)).Update(m)
}

func (o *oneToManyT) UpdateWithOptions(field, id interface{}, m map[string]interface{}, opts Options) error {
	return o.Where(Eq(o.fieldToIndexBy, field), Eq(o.idField, id)).UpdateWithOptions(m, opts)
}

func (o *oneToManyT) Delete(field, id interface{}) error {
	return o.Where(Eq(o.fieldToIndexBy, field), Eq(o.idField, id)).Delete()
}

func (o *oneToManyT) DeleteAll(field interface{}) error {
	return o.Where(Eq(o.fieldToIndexBy, field)).Delete()
}

func (o *oneToManyT) Read(field, id, pointer interface{}) error {
	return o.Where(Eq(o.fieldToIndexBy, field), Eq(o.idField, id)).Query().ReadOne(pointer)
}

func (o *oneToManyT) MultiRead(field interface{}, ids []interface{}, pointerToASlice interface{}) error {
	return o.Where(Eq(o.fieldToIndexBy, field), In(o.idField, ids...)).Query().Read(pointerToASlice)
}

func (o *oneToManyT) List(field, startId interface{}, limit int, pointerToASlice interface{}) error {
	return o.Where(Eq(o.fieldToIndexBy, field), GTE(o.idField, startId)).Query().Limit(limit).Read(pointerToASlice)
}
