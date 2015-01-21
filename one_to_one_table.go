package gocassa

import (
	"errors"
	"fmt"
)

type oneToOneT struct {
	*t
	idField string
}

func (o *oneToOneT) Update(id interface{}, m map[string]interface{}) error {
	return o.Where(Eq(o.idField, id)).Update(m)
}

func (o *oneToOneT) Delete(id interface{}) error {
	return o.Where(Eq(o.idField, id)).Delete()
}

func (o *oneToOneT) Read(id interface{}) (interface{}, error) {
	res, err := o.Where(Eq(o.idField, id)).Query().Read()
	if err != nil {
		return nil, err
	}
	if len(res) == 0 {
		return nil, errors.New(fmt.Sprintf("Row with id %v not found", id))
	}
	return res[0], nil
}
