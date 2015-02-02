package gocassa

import (
	"errors"
	"fmt"
	"time"
)

type timeSeriesBT struct {
	*t
	indexField string
	timeField  string
	idField    string
	bucketSize time.Duration
}

func (o *timeSeriesBT) SetWithOptions(v interface{}, opts Options) error {
	m, ok := toMap(v)
	if !ok {
		return errors.New("Can't set: not able to convert")
	}
	if tim, ok := m[o.timeField].(time.Time); !ok {
		return errors.New("timeField is not actually a time.Time")
	} else {
		m[bucketFieldName] = o.bucket(tim.Unix())
	}
	return o.t.SetWithOptions(m, opts)
}

func (o *timeSeriesBT) Set(v interface{}) error {
	return o.SetWithOptions(v, Options{})
}

func (o *timeSeriesBT) bucket(secs int64) int64 {
	return (secs - secs%int64(o.bucketSize/time.Second)) * 1000
}

func (o *timeSeriesBT) Update(v interface{}, timeStamp time.Time, id interface{}, m map[string]interface{}) error {
	bucket := o.bucket(timeStamp.Unix())
	return o.Where(Eq(o.indexField, v), Eq(bucketFieldName, bucket), Eq(o.timeField, timeStamp), Eq(o.idField, id)).Update(m)
}

func (o *timeSeriesBT) UpdateWithOptions(v interface{}, timeStamp time.Time, id interface{}, m map[string]interface{}, opts Options) error {
	bucket := o.bucket(timeStamp.Unix())
	return o.Where(Eq(o.indexField, v), Eq(bucketFieldName, bucket), Eq(o.timeField, timeStamp), Eq(o.idField, id)).UpdateWithOptions(m, opts)
}

func (o *timeSeriesBT) Delete(v interface{}, timeStamp time.Time, id interface{}) error {
	bucket := o.bucket(timeStamp.Unix())
	return o.Where(Eq(o.indexField, v), Eq(bucketFieldName, bucket), Eq(o.timeField, timeStamp), Eq(o.idField, id)).Delete()
}

func (o *timeSeriesBT) Read(v interface{}, timeStamp time.Time, id interface{}) (interface{}, error) {
	bucket := o.bucket(timeStamp.Unix())
	if res, err := o.Where(Eq(o.indexField, v), Eq(bucketFieldName, bucket), Eq(o.timeField, timeStamp), Eq(o.idField, id)).Query().Read(); err != nil {
		return nil, err
	} else if len(res) == 0 {
		return nil, fmt.Errorf("Row with id %v not found", id)
	} else {
		return res[0], nil
	}
}

func (o *timeSeriesBT) List(v interface{}, startTime time.Time, endTime time.Time) ([]interface{}, error) {
	buckets := []interface{}{}
	start := o.bucket(startTime.Unix())
	for i := start; ; i += int64(o.bucketSize/time.Second) * 1000 {
		if i >= endTime.Unix()*1000 {
			break
		}
		buckets = append(buckets, i)
	}
	return o.Where(Eq(o.indexField, v), In(bucketFieldName, buckets...), GTE(o.timeField, startTime), LTE(o.timeField, endTime)).Query().Read()
}
