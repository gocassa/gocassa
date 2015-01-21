package cmagic

import (
	"errors"
	"fmt"
)

type oneToManyT struct {
	*t
	fieldToIndexBy string
	idField        string
}

func (o *oneToManyT) Update(field, id interface{}, m map[string]interface{}) error {
	return o.Where(Eq(o.fieldToIndexBy, field), Eq(o.idField, id)).Update(m)
}

func (o *oneToManyT) Delete(field, id interface{}) error {
	return o.Where(Eq(o.fieldToIndexBy, field), Eq(o.idField, id)).Delete()
}

func (o *oneToManyT) DeleteAll(field interface{}) error {
	return o.Where(Eq(o.fieldToIndexBy, field)).Delete()
}

func (o *oneToManyT) Read(field, id interface{}) (interface{}, error) {
	res, err := o.Where(Eq(o.fieldToIndexBy, field), Eq(o.idField, id)).Query().Read()
	if err != nil {
		return nil, err
	}
	if len(res) == 0 {
		return nil, errors.New(fmt.Sprintf("Row with id %v not found", id))
	}
	return res[0], nil
}

func (o *oneToManyT) List(field, startId interface{}, limit int) ([]interface{}, error) {
	return o.Where(Eq(o.fieldToIndexBy, field), GTE(o.idField, startId)).Query().Limit(limit).Read()
}
