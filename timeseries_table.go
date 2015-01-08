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

type timeSeriesTable struct {
	t Table
	uuidField string
	bucketSize time.Time
}

func (o *timeSeriesTable) Set(v interface{}) error {
	m, ok := toMap(v)
	if !ok {
		return errors.New("Can't set: not able to convert")
	}
	tim, ok := m[o.uuidField].(TimeUUID)
	if !ok {
		return errors.New("timeuuidField is not actually timeuuidField")
	}
	m["_bucket"] = tim.v.Time().UnixNano()/o.bucketSize.UnixNano()
	return o.t.Set(m)
}

func (o *timeSeriesTable) Update(id TimeUUID, m map[string]interface{}) error {
	return o.t.Where(Eq(o.uuidField, id)).Update(m)
}

func (o *timeSeriesTable) Delete(id TimeUUID) error {
	return o.t.Where(Eq(o.uuidField, id)).Delete()
}

func (o *timeSeriesTable) Read(id TimeUUID) (interface{}, error) {
	res, err := o.t.Where(Eq(o.uuidField, id)).Query().Read()
	if err != nil {
		return nil, err
	}
	if len(res) == 0 {
		return nil, errors.New(fmt.Sprintf("Row with id %v not found", id))
	}
	return res[0], nil
}

func (o *timeSeriesTable) List(startTime time.Time, endTime time.Time) ([]interface{}, error) {
	return nil, nil
}