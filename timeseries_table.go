package cmagic 

import(
	"fmt"
	"errors"
	"github.com/gocql/gocql"
	"time"
)

const bucketFieldName = "bucket"

type TimeUUID struct {
	v gocql.UUID
}

func (t TimeUUID) Time() time.Time {
	return t.v.Time()
}

func (t TimeUUID) String() string {
	return ""
}

func NewTimeUUID() TimeUUID {
	return TimeUUID{
		v: gocql.TimeUUID(),
	}
}

type timeSeriesTable struct {
	t Table
	timeField string
	idField string
	bucketSize time.Duration
}

func (o *timeSeriesTable) Set(v interface{}) error {
	m, ok := toMap(v)
	if !ok {
		return errors.New("Can't set: not able to convert")
	}
	tim, ok := m[o.timeField].(time.Time)
	if !ok {
		return errors.New("timeField is not actually a time.Time")
	}
	m[bucketFieldName] = tim.UnixNano()/int64(o.bucketSize)
	return o.t.Set(m)
}

func (o *timeSeriesTable) bucket(t time.Time) int64 {
	return t.UnixNano()/int64(o.bucketSize)
}

func (o *timeSeriesTable) Update(timeStamp time.Time, id interface{}, m map[string]interface{}) error {
	bucket := o.bucket(timeStamp)
	return o.t.Where(Eq(bucketFieldName, bucket), Eq(o.timeField, timeStamp), Eq(o.idField, id)).Update(m)
}

func (o *timeSeriesTable) Delete(timeStamp time.Time, id interface{}) error {
	bucket := o.bucket(timeStamp)
	return o.t.Where(Eq(bucketFieldName, bucket), Eq(o.timeField, timeStamp), Eq(o.idField, id)).Delete()
}

func (o *timeSeriesTable) Read(timeStamp time.Time, id interface{}) (interface{}, error) {
	bucket := o.bucket(timeStamp)
	res, err := o.t.Where(Eq(bucketFieldName, bucket), Eq(o.timeField, timeStamp), Eq(o.idField, id)).Query().Read()
	if err != nil {
		return nil, err
	}
	if len(res) == 0 {
		return nil, errors.New(fmt.Sprintf("Row with id %v not found", id))
	}
	return res[0], nil
}

func (o *timeSeriesTable) List(startTime time.Time, endTime time.Time) ([]interface{}, error) {
	buckets := []interface{}{}
	for i:=startTime.UnixNano();i<endTime.UnixNano();i+=int64(o.bucketSize) {
		buckets = append(buckets, i/int64(o.bucketSize))
	}
	return o.t.Where(In(bucketFieldName, buckets...), GTE(o.timeField, startTime), LTE(o.timeField, endTime)).Query().Read()
}