package cmagic 

import(
	"fmt"
	"errors"
)

type OneToManyT struct {
	*T
	fieldToIndexBy string 
	idField string
}

func (o *OneToManyT) Update(field, id interface{}, m map[string]interface{}) error {
	return o.T.Where(Eq(o.fieldToIndexBy, field), Eq(o.idField, id)).Update(m)
}

func (o *OneToManyT) Delete(field, id interface{}) error {
	return o.T.Where(Eq(o.fieldToIndexBy, field), Eq(o.idField, id)).Delete()
}

func (o *OneToManyT) Read(field, id interface{}) (interface{}, error) {
	res, err := o.T.Where(Eq(o.fieldToIndexBy, field), Eq(o.idField, id)).Query().Read()
	if err != nil {
		return nil, err
	}
	if len(res) == 0 {
		return nil, errors.New(fmt.Sprintf("Row with id %v not found", id))
	}
	return res[0], nil
}

func (o *OneToManyT) List(field, startId interface{}, limit int) ([]interface{}, error) {
	return o.T.Where(Eq(o.fieldToIndexBy, field), GTE(o.idField, startId)).Query().Limit(limit).Read()
}
