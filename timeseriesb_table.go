package cmagic 

import(
	"fmt"
	"errors"
	"time"
)

type timeSeriesBTable struct {
	t Table
	indexField string
	timeField string
	idField string
	bucketSize time.Duration
}

func (o *timeSeriesBTable) Set(v interface{}) error {
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

func (o *timeSeriesBTable) bucket(secs int64) int64 {
	return secs - secs%int64(o.bucketSize/time.Second)
}

func (o *timeSeriesBTable) Update(v interface{}, timeStamp time.Time, id interface{}, m map[string]interface{}) error {
	bucket := o.bucket(timeStamp.Unix())
	return o.t.Where(Eq(o.indexField, v), Eq(bucketFieldName, bucket), Eq(o.timeField, timeStamp), Eq(o.idField, id)).Update(m)
}

func (o *timeSeriesBTable) Delete(v interface{}, timeStamp time.Time, id interface{}) error {
	bucket := o.bucket(timeStamp.Unix())
	return o.t.Where(Eq(o.indexField, v), Eq(bucketFieldName, bucket), Eq(o.timeField, timeStamp), Eq(o.idField, id)).Delete()
}

func (o *timeSeriesBTable) Read(v interface{}, timeStamp time.Time, id interface{}) (interface{}, error) {
	bucket := o.bucket(timeStamp.Unix())
	res, err := o.t.Where(Eq(o.indexField, v), Eq(bucketFieldName, bucket), Eq(o.timeField, timeStamp), Eq(o.idField, id)).Query().Read()
	if err != nil {
		return nil, err
	}
	if len(res) == 0 {
		return nil, errors.New(fmt.Sprintf("Row with id %v not found", id))
	}
	return res[0], nil
}

func (o *timeSeriesBTable) List(v interface{}, startTime time.Time, endTime time.Time) ([]interface{}, error) {
	buckets := []interface{}{}
	start := o.bucket(startTime.Unix())
	for i:=start;;i+=int64(o.bucketSize/time.Second) {
		buckets = append(buckets, o.bucket(i))
		if i>=endTime.Unix() {
			break
		}
	}
	return o.t.Where(Eq(o.indexField, v), In(bucketFieldName, buckets...), GTE(o.timeField, startTime), LTE(o.timeField, endTime)).Query().Read()
}