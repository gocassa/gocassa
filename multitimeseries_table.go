package gocassa

import (
	"time"
)

type multiTimeSeriesT struct {
	*t
	indexField string
	timeField  string
	idField    string
	bucketSize time.Duration
}

func (o *multiTimeSeriesT) SetWithOptions(v interface{}, opts Options) Op {
	m, ok := toMap(v)
	if !ok {
		panic("Can't set: not able to convert")
	}
	if tim, ok := m[o.timeField].(time.Time); !ok {
		panic("timeField is not actually a time.Time")
	} else {
		m[bucketFieldName] = o.bucket(tim.Unix())
	}
	return o.t.SetWithOptions(m, opts)
}

func (o *multiTimeSeriesT) Set(v interface{}) Op {
	return o.SetWithOptions(v, Options{})
}

func (o *multiTimeSeriesT) bucket(secs int64) int64 {
	return (secs - secs%int64(o.bucketSize/time.Second)) * 1000
}

func (o *multiTimeSeriesT) Update(v interface{}, timeStamp time.Time, id interface{}, m map[string]interface{}) Op {
	bucket := o.bucket(timeStamp.Unix())
	return o.Where(Eq(o.indexField, v), Eq(bucketFieldName, bucket), Eq(o.timeField, timeStamp), Eq(o.idField, id)).Update(m)
}

func (o *multiTimeSeriesT) UpdateWithOptions(v interface{}, timeStamp time.Time, id interface{}, m map[string]interface{}, opts Options) Op {
	bucket := o.bucket(timeStamp.Unix())
	return o.Where(Eq(o.indexField, v), Eq(bucketFieldName, bucket), Eq(o.timeField, timeStamp), Eq(o.idField, id)).UpdateWithOptions(m, opts)
}

func (o *multiTimeSeriesT) Delete(v interface{}, timeStamp time.Time, id interface{}) Op {
	bucket := o.bucket(timeStamp.Unix())
	return o.Where(Eq(o.indexField, v), Eq(bucketFieldName, bucket), Eq(o.timeField, timeStamp), Eq(o.idField, id)).Delete()
}

func (o *multiTimeSeriesT) Read(v interface{}, timeStamp time.Time, id, pointer interface{}) Op {
	bucket := o.bucket(timeStamp.Unix())
	return o.Where(Eq(o.indexField, v), Eq(bucketFieldName, bucket), Eq(o.timeField, timeStamp), Eq(o.idField, id)).Query().ReadOne(pointer)

}

func (o *multiTimeSeriesT) List(v interface{}, startTime time.Time, endTime time.Time, pointerToASlice interface{}) Op {
	buckets := []interface{}{}
	start := o.bucket(startTime.Unix())
	for i := start; ; i += int64(o.bucketSize/time.Second) * 1000 {
		if i >= endTime.Unix()*1000 {
			break
		}
		buckets = append(buckets, i)
	}
	return o.Where(Eq(o.indexField, v), In(bucketFieldName, buckets...), GTE(o.timeField, startTime), LTE(o.timeField, endTime)).Query().Read(pointerToASlice)
}

func (o *multiTimeSeriesT) WithOptions(opt Options) MultiTimeSeriesTable {
	tee := o.t.WithOptions(opt).(t)
	return &multiTimeSeriesT{
		t:          &tee,
		indexField: o.indexField,
		idField:    o.idField,
		bucketSize: o.bucketSize,
	}
}
