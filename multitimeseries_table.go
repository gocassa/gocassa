package gocassa

import (
	"time"
)

type multiTimeSeriesT struct {
	t          Table
	indexField string
	timeField  string
	idField    string
	bucketSize time.Duration
}

func (o *multiTimeSeriesT) Table() Table                     { return o.t }
func (o *multiTimeSeriesT) Create() error                    { return o.Table().Create() }
func (o *multiTimeSeriesT) CreateIfNotExist() error          { return o.Table().CreateIfNotExist() }
func (o *multiTimeSeriesT) Name() string                     { return o.Table().Name() }
func (o *multiTimeSeriesT) Recreate() error                  { return o.Table().Recreate() }
func (o *multiTimeSeriesT) CreateStatement() (string, error) { return o.Table().CreateStatement() }
func (o *multiTimeSeriesT) CreateIfNotExistStatement() (string, error) {
	return o.Table().CreateIfNotExistStatement()
}

func (o *multiTimeSeriesT) Set(v interface{}) Op {
	m, ok := toMap(v)
	if !ok {
		panic("Can't set: not able to convert")
	}
	if tim, ok := m[o.timeField].(time.Time); !ok {
		panic("timeField is not actually a time.Time")
	} else {
		m[bucketFieldName] = o.bucket(tim.Unix())
	}
	return o.Table().
		Set(m)
}

func (o *multiTimeSeriesT) bucket(secs int64) int64 {
	return (secs - secs%int64(o.bucketSize/time.Second)) * 1000
}

func (o *multiTimeSeriesT) Update(v interface{}, timeStamp time.Time, id interface{}, m map[string]interface{}) Op {
	bucket := o.bucket(timeStamp.Unix())
	return o.Table().
		Where(Eq(o.indexField, v),
			Eq(bucketFieldName, bucket),
			Eq(o.timeField, timeStamp),
			Eq(o.idField, id)).
		Update(m)
}

func (o *multiTimeSeriesT) Delete(v interface{}, timeStamp time.Time, id interface{}) Op {
	bucket := o.bucket(timeStamp.Unix())
	return o.Table().
		Where(Eq(o.indexField, v),
			Eq(bucketFieldName, bucket),
			Eq(o.timeField, timeStamp),
			Eq(o.idField, id)).
		Delete()
}

func (o *multiTimeSeriesT) Read(v interface{}, timeStamp time.Time, id, pointer interface{}) Op {
	bucket := o.bucket(timeStamp.Unix())
	return o.Table().
		Where(Eq(o.indexField, v),
			Eq(bucketFieldName, bucket),
			Eq(o.timeField, timeStamp),
			Eq(o.idField, id)).
		ReadOne(pointer)

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
	return o.Table().
		Where(Eq(o.indexField, v),
			In(bucketFieldName, buckets...),
			GTE(o.timeField, startTime),
			LTE(o.timeField, endTime)).
		Read(pointerToASlice)
}

func (o *multiTimeSeriesT) WithOptions(opt Options) MultiTimeSeriesTable {
	return &multiTimeSeriesT{
		t:          o.Table().WithOptions(opt),
		indexField: o.indexField,
		timeField:  o.timeField,
		idField:    o.idField,
		bucketSize: o.bucketSize,
	}
}
