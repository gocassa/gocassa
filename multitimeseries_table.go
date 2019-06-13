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

func (o *multiTimeSeriesT) Table() Table                        { return o.t }
func (o *multiTimeSeriesT) Create() error                       { return o.Table().Create() }
func (o *multiTimeSeriesT) CreateIfNotExist() error             { return o.Table().CreateIfNotExist() }
func (o *multiTimeSeriesT) Name() string                        { return o.Table().Name() }
func (o *multiTimeSeriesT) Recreate() error                     { return o.Table().Recreate() }
func (o *multiTimeSeriesT) CreateStatement() (Statement, error) { return o.Table().CreateStatement() }
func (o *multiTimeSeriesT) CreateIfNotExistStatement() (Statement, error) {
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
		m[bucketFieldName] = bucket(tim, o.bucketSize)
	}
	return o.Table().
		Set(m)
}

func (o *multiTimeSeriesT) Update(v interface{}, timeStamp time.Time, id interface{}, m map[string]interface{}) Op {
	bucket := bucket(timeStamp, o.bucketSize)
	return o.Table().
		Where(Eq(o.indexField, v),
			Eq(bucketFieldName, bucket),
			Eq(o.timeField, timeStamp),
			Eq(o.idField, id)).
		Update(m)
}

func (o *multiTimeSeriesT) Delete(v interface{}, timeStamp time.Time, id interface{}) Op {
	bucket := bucket(timeStamp, o.bucketSize)
	return o.Table().
		Where(Eq(o.indexField, v),
			Eq(bucketFieldName, bucket),
			Eq(o.timeField, timeStamp),
			Eq(o.idField, id)).
		Delete()
}

func (o *multiTimeSeriesT) Read(v interface{}, timeStamp time.Time, id, pointer interface{}) Op {
	bucket := bucket(timeStamp, o.bucketSize)
	return o.Table().
		Where(Eq(o.indexField, v),
			Eq(bucketFieldName, bucket),
			Eq(o.timeField, timeStamp),
			Eq(o.idField, id)).
		ReadOne(pointer)

}

func (o *multiTimeSeriesT) List(v interface{}, startTime time.Time, endTime time.Time, pointerToASlice interface{}) Op {
	buckets := []interface{}{}
	for bucket := o.Buckets(v, startTime); bucket.Bucket().Before(endTime); bucket = bucket.Next() {
		buckets = append(buckets, bucket.Bucket())
	}
	return o.Table().
		Where(Eq(o.indexField, v),
			In(bucketFieldName, buckets...),
			GTE(o.timeField, startTime),
			LTE(o.timeField, endTime)).
		Read(pointerToASlice)
}

func (o *multiTimeSeriesT) Buckets(v interface{}, start time.Time) Buckets {
	return bucketIter{
		v:         start,
		step:      o.bucketSize,
		field:     bucketFieldName,
		invariant: o.Table().Where(Eq(o.indexField, v))}
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
