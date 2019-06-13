package gocassa

import (
	"time"
)

const bucketFieldName = "bucket"

type timeSeriesT struct {
	t          Table
	timeField  string
	idField    string
	bucketSize time.Duration
}

func (o *timeSeriesT) Table() Table                        { return o.t }
func (o *timeSeriesT) Create() error                       { return o.Table().Create() }
func (o *timeSeriesT) CreateIfNotExist() error             { return o.Table().CreateIfNotExist() }
func (o *timeSeriesT) Name() string                        { return o.Table().Name() }
func (o *timeSeriesT) Recreate() error                     { return o.Table().Recreate() }
func (o *timeSeriesT) CreateStatement() (Statement, error) { return o.Table().CreateStatement() }
func (o *timeSeriesT) CreateIfNotExistStatement() (Statement, error) {
	return o.Table().CreateIfNotExistStatement()
}

func (o *timeSeriesT) Set(v interface{}) Op {
	m, ok := toMap(v)
	if !ok {
		panic("Can't set: not able to convert")
	}
	if tim, ok := m[o.timeField].(time.Time); !ok {
		panic("timeField is not actually a time.Time")
	} else {
		m[bucketFieldName] = bucket(tim, o.bucketSize)
	}
	return o.Table().Set(m)
}

func (o *timeSeriesT) Update(timeStamp time.Time, id interface{}, m map[string]interface{}) Op {
	bucket := bucket(timeStamp, o.bucketSize)
	return o.Table().
		Where(Eq(bucketFieldName, bucket),
			Eq(o.timeField, timeStamp),
			Eq(o.idField, id)).
		Update(m)
}

func (o *timeSeriesT) Delete(timeStamp time.Time, id interface{}) Op {
	bucket := bucket(timeStamp, o.bucketSize)
	return o.Table().
		Where(Eq(bucketFieldName, bucket),
			Eq(o.timeField, timeStamp),
			Eq(o.idField, id)).
		Delete()
}

func (o *timeSeriesT) Read(timeStamp time.Time, id, pointer interface{}) Op {
	bucket := bucket(timeStamp, o.bucketSize)
	return o.Table().
		Where(Eq(bucketFieldName, bucket),
			Eq(o.timeField, timeStamp),
			Eq(o.idField, id)).
		ReadOne(pointer)
}

func (o *timeSeriesT) List(startTime time.Time, endTime time.Time, pointerToASlice interface{}) Op {
	buckets := []interface{}{}
	for bucket := o.Buckets(startTime); bucket.Bucket().Before(endTime); bucket = bucket.Next() {
		buckets = append(buckets, bucket.Bucket())
	}
	return o.Table().
		Where(In(bucketFieldName, buckets...),
			GTE(o.timeField, startTime),
			LTE(o.timeField, endTime)).
		Read(pointerToASlice)
}

func (o *timeSeriesT) Buckets(start time.Time) Buckets {
	return bucketIter{
		v:         start,
		step:      o.bucketSize,
		field:     bucketFieldName,
		invariant: o.Table().Where()}
}

func (o *timeSeriesT) WithOptions(opt Options) TimeSeriesTable {
	return &timeSeriesT{
		t:          o.Table().WithOptions(opt),
		timeField:  o.timeField,
		idField:    o.idField,
		bucketSize: o.bucketSize,
	}
}
