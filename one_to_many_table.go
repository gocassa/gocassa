package cmagic 

import(
	"fmt"
	"errors"
)

type oneToMany struct {
	t Table
	fieldToIndexBy string 
	idField string
}

func (o *oneToMany) Set(v interface{}) error {
	return o.t.Set(v)
}

func (o *oneToMany) Update(field, id interface{}, m map[string]interface{}) error {
	return o.t.Where(Eq(o.fieldToIndexBy, field), Eq(o.idField, id)).Update(m)
}

func (o *oneToMany) Delete(field, id interface{}) error {
	return o.t.Where(Eq(o.fieldToIndexBy, field), Eq(o.idField, id)).Delete()
}

func (o *oneToMany) Read(field, id interface{}) (interface{}, error) {
	res, err := o.t.Where(Eq(o.fieldToIndexBy, field), Eq(o.idField, id)).Query().Read()
	if err != nil {
		return nil, err
	}
	if len(res) == 0 {
		return nil, errors.New(fmt.Sprintf("Row with id %v not found", id))
	}
	return res[0], nil
}

func (o *oneToMany) List(field, startId interface{}, limit int) ([]interface{}, error) {
	return o.t.Where(Eq(o.fieldToIndexBy, field), GTE(o.idField, startId)).Query().Limit(limit).Read()
}
