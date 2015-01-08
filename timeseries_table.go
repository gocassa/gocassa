package cmagic 

import(
	"fmt"
	"errors"
	"github.com/gocql/gocql"
	"time"
)

type TimeUUID struct {
	v gocql.UUID
}

func (t TimeUUID) Time() time.Time {
	return t.v.Time()
}

func NewTimeUUID() TimeUUID {
	return TimeUUID{
		v: gocql.TimeUUID(),
	}
}

type timeSeries struct {
	t Table
	idField string
}

func (o *timeSeries) Set(v interface{}) error {
	return o.t.Set(v)
}

func (o *timeSeries) Update(id interface{}, m map[string]interface{}) error {
	return o.t.Where(Eq(o.idField, id)).Update(m)
}

func (o *timeSeries) Delete(id interface{}) error {
	return o.t.Where(Eq(o.idField, id)).Delete()
}

func (o *timeSeries) Read(id interface{}) (interface{}, error) {
	res, err := o.t.Where(Eq(o.idField, id)).Query().Read()
	if err != nil {
		return nil, err
	}
	if len(res) == 0 {
		return nil, errors.New(fmt.Sprintf("Row with id %v not found", id))
	}
	return res[0], nil
}