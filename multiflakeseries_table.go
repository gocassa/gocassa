package gocassa

import (
	"fmt"
	"time"
)

type multiFlakeSeriesT struct {
	t          Table
	indexField string
	idField    string
	bucketSize time.Duration
}

func (o *multiFlakeSeriesT) Table() Table                        { return o.t }
func (o *multiFlakeSeriesT) Create() error                       { return o.Table().Create() }
func (o *multiFlakeSeriesT) CreateIfNotExist() error             { return o.Table().CreateIfNotExist() }
func (o *multiFlakeSeriesT) Name() string                        { return o.Table().Name() }
func (o *multiFlakeSeriesT) Recreate() error                     { return o.Table().Recreate() }
func (o *multiFlakeSeriesT) CreateStatement() (Statement, error) { return o.Table().CreateStatement() }
func (o *multiFlakeSeriesT) CreateIfNotExistStatement() (Statement, error) {
	return o.Table().CreateIfNotExistStatement()
}

func (o *multiFlakeSeriesT) Set(v interface{}) Op {
	m, ok := toMap(v)
	if !ok {
		panic("Can't set: not able to convert")
	}
	id, ok := m[o.idField].(string)
	if !ok {
		panic(fmt.Sprintf("Id field (%s) is not present or is not a string", o.idField))
	}

	timestamp, err := flakeToTime(id)
	if err != nil {
		return errOp{err: err}
	}

	m[flakeTimestampFieldName] = timestamp
	m[bucketFieldName] = bucket(timestamp, o.bucketSize)

	return o.Table().
		Set(m)
}

func (o *multiFlakeSeriesT) Update(v interface{}, id string, m map[string]interface{}) Op {
	timestamp, err := flakeToTime(id)
	if err != nil {
		return errOp{err: err}
	}
	bucket := bucket(timestamp, o.bucketSize)

	return o.Table().
		Where(Eq(o.indexField, v),
			Eq(bucketFieldName, bucket),
			Eq(flakeTimestampFieldName, timestamp),
			Eq(o.idField, id)).
		Update(m)
}

func (o *multiFlakeSeriesT) Delete(v interface{}, id string) Op {
	timestamp, err := flakeToTime(id)
	if err != nil {
		return errOp{err: err}
	}
	bucket := bucket(timestamp, o.bucketSize)

	return o.Table().
		Where(Eq(o.indexField, v),
			Eq(bucketFieldName, bucket),
			Eq(flakeTimestampFieldName, timestamp),
			Eq(o.idField, id)).
		Delete()
}

func (o *multiFlakeSeriesT) Read(v interface{}, id string, pointer interface{}) Op {
	timestamp, err := flakeToTime(id)
	if err != nil {
		return errOp{err: err}
	}
	bucket := bucket(timestamp, o.bucketSize)
	return o.Table().
		Where(Eq(o.indexField, v),
			Eq(bucketFieldName, bucket),
			Eq(flakeTimestampFieldName, timestamp),
			Eq(o.idField, id)).
		ReadOne(pointer)
}

func (o *multiFlakeSeriesT) List(v interface{}, startTime, endTime time.Time, pointerToASlice interface{}) Op {
	buckets := []interface{}{}
	for bucket := o.Buckets(v, startTime); bucket.Bucket().Before(endTime); bucket = bucket.Next() {
		buckets = append(buckets, bucket.Bucket())
	}
	return o.Table().
		Where(Eq(o.indexField, v),
			In(bucketFieldName, buckets...),
			GTE(flakeTimestampFieldName, startTime),
			LT(flakeTimestampFieldName, endTime)).
		Read(pointerToASlice)
}

func (o *multiFlakeSeriesT) Buckets(v interface{}, start time.Time) Buckets {
	return bucketIter{
		v:         start,
		step:      o.bucketSize,
		field:     bucketFieldName,
		invariant: o.Table().Where(Eq(o.indexField, v))}
}

func (o *multiFlakeSeriesT) ListSince(v interface{}, id string, window time.Duration, pointerToASlice interface{}) Op {
	startTime, err := flakeToTime(id)
	if err != nil {
		return errOp{err: err}
	}

	var endTime time.Time
	if window == 0 {
		// no window set - so go up until 5 mins in the future
		endTime = time.Now().Add(5 * time.Minute)
	} else {
		endTime = startTime.Add(window)
	}

	buckets := []interface{}{}
	for bucket := o.Buckets(v, startTime); bucket.Bucket().Before(endTime); bucket = bucket.Next() {
		buckets = append(buckets, bucket.Bucket())
	}

	return o.Table().
		Where(Eq(o.indexField, v),
			In(bucketFieldName, buckets),
			GTE(flakeTimestampFieldName, startTime),
			LT(flakeTimestampFieldName, endTime),
			GT(o.idField, id)).
		Read(pointerToASlice)
}

func (o *multiFlakeSeriesT) WithOptions(opt Options) MultiFlakeSeriesTable {
	return &multiFlakeSeriesT{
		t:          o.Table().WithOptions(opt),
		indexField: o.indexField,
		idField:    o.idField,
		bucketSize: o.bucketSize,
	}
}
