package gocassa

import (
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

func (o *oneToManyT) UpdateWithOptions(field, id interface{}, m map[string]interface{}, opts Options) error {
	return o.Where(Eq(o.fieldToIndexBy, field), Eq(o.idField, id)).UpdateWithOptions(m, opts)
}

func (o *oneToManyT) Delete(field, id interface{}) error {
	return o.Where(Eq(o.fieldToIndexBy, field), Eq(o.idField, id)).Delete()
}

func (o *oneToManyT) DeleteAll(field interface{}) error {
	return o.Where(Eq(o.fieldToIndexBy, field)).Delete()
}

func (o *oneToManyT) readQuery(field, id interface{}) {

}

func (o *oneToManyT) Read(field, id interface{}) (interface{}, error) {
	if res, err := o.Where(Eq(o.fieldToIndexBy, field), Eq(o.idField, id)).Query().Read(); err != nil {
		return nil, err
	} else if len(res) == 0 {
		return nil, fmt.Errorf("Row with id %v not found", id)
	} else {
		return res[0], nil
	}
}

func (o *oneToManyT) ReadInto(field, id, pointer interface{}) (interface{}, error) {
	if res, err := o.Where(Eq(o.fieldToIndexBy, field), Eq(o.idField, id)).Query().Read(); err != nil {
		return nil, err
	} else if len(res) == 0 {
		return nil, fmt.Errorf("Row with id %v not found", id)
	} else {
		return res[0], nil
	}
}

func (o *oneToManyT) multiReadQuery(field interface{}, ids ...interface{}) Query {

}

func (o *oneToManyT) MultiRead(field interface{}, ids ...interface{}) ([]interface{}, error) {
	return o.Where(Eq(o.fieldToIndexBy, field), In(o.idField, ids...)).Query().Read()
}

func (o *oneToMany)

func (o *oneToManyT) List(field, startId interface{}, limit int) ([]interface{}, error) {
	return o.Where(Eq(o.fieldToIndexBy, field), GTE(o.idField, startId)).Query().Limit(limit).Read()
}
