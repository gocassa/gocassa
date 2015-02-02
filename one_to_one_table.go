package gocassa

import (
	"fmt"
)

type oneToOneT struct {
	*t
	idField string
}

func (o *oneToOneT) Update(id interface{}, m map[string]interface{}) error {
	return o.Where(Eq(o.idField, id)).Update(m)
}

func (o *oneToOneT) UpdateWithOptions(id interface{}, m map[string]interface{}, opts Options) error {
	return o.Where(Eq(o.idField, id)).UpdateWithOptions(m, opts)
}

func (o *oneToOneT) Delete(id interface{}) error {
	return o.Where(Eq(o.idField, id)).Delete()
}

func (o *oneToOneT) Read(id interface{}) (interface{}, error) {
	if res, err := o.Where(Eq(o.idField, id)).Query().Read(); err != nil {
		return nil, err
	} else if len(res) == 0 {
		return nil, fmt.Errorf("Row with id %v not found", id)
	} else {
		return res[0], nil
	}
}

func (o *oneToOneT) MultiRead(ids ...interface{}) ([]interface{}, error) {
	return o.Where(In(o.idField, ids...)).Query().Read()
}
