package gocassa

import (
	"errors"
	"time"
)

const bucketFieldName = "bucket"

type timeSeriesT struct {
	Table
	timeField  string
	idField    string
	bucketSize time.Duration
}

func (o *timeSeriesT) SetWithOptions(v interface{}, opts Options) error {
	m, ok := toMap(v)
	if !ok {
		return errors.New("Can't set: not able to convert")
	}
	if tim, ok := m[o.timeField].(time.Time); !ok {
		return errors.New("timeField is not actually a time.Time")
	} else {
		m[bucketFieldName] = o.bucket(tim.Unix())
	}
	return o.Table.SetWithOptions(m, opts)
}

func (o *timeSeriesT) Set(v interface{}) error {
	return o.SetWithOptions(v, Options{})
}

func (o *timeSeriesT) bucket(secs int64) int64 {
	return (secs - secs%int64(o.bucketSize/time.Second)) * 1000
}

func (o *timeSeriesT) Update(timeStamp time.Time, id interface{}, m map[string]interface{}) error {
	bucket := o.bucket(timeStamp.Unix())
	return o.Where(Eq(bucketFieldName, bucket), Eq(o.timeField, timeStamp), Eq(o.idField, id)).Update(m)
}

func (o *timeSeriesT) UpdateWithOptions(timeStamp time.Time, id interface{}, m map[string]interface{}, opts Options) error {
	bucket := o.bucket(timeStamp.Unix())
	return o.Where(Eq(bucketFieldName, bucket), Eq(o.timeField, timeStamp), Eq(o.idField, id)).UpdateWithOptions(m, opts)
}

func (o *timeSeriesT) Delete(timeStamp time.Time, id interface{}) error {
	bucket := o.bucket(timeStamp.Unix())
	return o.Where(Eq(bucketFieldName, bucket), Eq(o.timeField, timeStamp), Eq(o.idField, id)).Delete()
}

func (o *timeSeriesT) Read(timeStamp time.Time, id, pointer interface{}) error {
	bucket := o.bucket(timeStamp.Unix())
	return o.Where(Eq(bucketFieldName, bucket), Eq(o.timeField, timeStamp), Eq(o.idField, id)).Query().ReadOne(pointer)
}

func (o *timeSeriesT) List(startTime time.Time, endTime time.Time, pointerToASlice interface{}) error {
	buckets := []interface{}{}
	start := o.bucket(startTime.Unix())
	for i := start; ; i += int64(o.bucketSize/time.Second) * 1000 {
		if i >= endTime.Unix()*1000 {
			break
		}
		buckets = append(buckets, i)
	}
	return o.Where(In(bucketFieldName, buckets...), GTE(o.timeField, startTime), LTE(o.timeField, endTime)).Query().Read(pointerToASlice)
}
