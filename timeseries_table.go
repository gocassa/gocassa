package cmagic 

import(
	"fmt"
	"errors"
	"time"
)

const bucketFieldName = "bucket"

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
	m[bucketFieldName] = o.bucket(tim.Unix())
	return o.t.Set(m)
}

func (o *timeSeriesTable) bucket(secs int64) int64 {
	return (secs - secs%int64(o.bucketSize/time.Second)) * 1000
}

func (o *timeSeriesTable) Update(timeStamp time.Time, id interface{}, m map[string]interface{}) error {
	bucket := o.bucket(timeStamp.Unix())
	return o.t.Where(Eq(bucketFieldName, bucket), Eq(o.timeField, timeStamp), Eq(o.idField, id)).Update(m)
}

func (o *timeSeriesTable) Delete(timeStamp time.Time, id interface{}) error {
	bucket := o.bucket(timeStamp.Unix())
	return o.t.Where(Eq(bucketFieldName, bucket), Eq(o.timeField, timeStamp), Eq(o.idField, id)).Delete()
}

func (o *timeSeriesTable) Read(timeStamp time.Time, id interface{}) (interface{}, error) {
	bucket := o.bucket(timeStamp.Unix())
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
	start := o.bucket(startTime.Unix())
	for i:=start;;i+=int64(o.bucketSize/time.Second)*1000 {
		buckets = append(buckets, i)
		if i>=endTime.Unix()*1000 {
			break
		}
	}
	return o.t.Where(In(bucketFieldName, buckets...), GTE(o.timeField, startTime), LTE(o.timeField, endTime)).Query().Read()
}