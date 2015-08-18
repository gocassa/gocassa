package gocassa

import (
	"fmt"
	"time"
)

type multiFlakeSeriesT struct {
	Table
	indexField string
	idField    string
	bucketSize time.Duration
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
	m[bucketFieldName] = o.bucket(timestamp.Unix())

	return o.Table.Set(m)
}

func (o *multiFlakeSeriesT) Update(v interface{}, id string, m map[string]interface{}) Op {
	timestamp, err := flakeToTime(id)
	if err != nil {
		return errOp{err: err}
	}
	bucket := o.bucket(timestamp.Unix())

	return o.Where(Eq(o.indexField, v), Eq(bucketFieldName, bucket), Eq(flakeTimestampFieldName, timestamp), Eq(o.idField, id)).Update(m)
}

func (o *multiFlakeSeriesT) Delete(v interface{}, id string) Op {
	timestamp, err := flakeToTime(id)
	if err != nil {
		return errOp{err: err}
	}
	bucket := o.bucket(timestamp.Unix())

	return o.Where(Eq(o.indexField, v), Eq(bucketFieldName, bucket), Eq(flakeTimestampFieldName, timestamp), Eq(o.idField, id)).Delete()
}

func (o *multiFlakeSeriesT) Read(v interface{}, id string, pointer interface{}) Op {
	timestamp, err := flakeToTime(id)
	if err != nil {
		return errOp{err: err}
	}
	bucket := o.bucket(timestamp.Unix())
	return o.Where(Eq(o.indexField, v), Eq(bucketFieldName, bucket), Eq(flakeTimestampFieldName, timestamp), Eq(o.idField, id)).ReadOne(pointer)
}

func (o *multiFlakeSeriesT) List(v interface{}, startTime, endTime time.Time, pointerToASlice interface{}) Op {
	buckets := o.buckets(startTime, endTime)
	return o.Where(Eq(o.indexField, v), In(bucketFieldName, buckets...), GTE(flakeTimestampFieldName, startTime), LT(flakeTimestampFieldName, endTime)).Read(pointerToASlice)
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
	buckets := o.buckets(startTime, endTime)
	return o.Where(Eq(o.indexField, v), In(bucketFieldName, buckets), GTE(flakeTimestampFieldName, startTime), LT(flakeTimestampFieldName, endTime), GT(o.idField, id)).Read(pointerToASlice)
}

func (o *multiFlakeSeriesT) WithOptions(opt Options) MultiFlakeSeriesTable {
	return &multiFlakeSeriesT{
		Table:      o.Table.WithOptions(opt),
		bucketSize: o.bucketSize,
		indexField: o.indexField,
	}
}

func (o *multiFlakeSeriesT) buckets(startTime, endTime time.Time) []interface{} {
	buckets := []interface{}{}
	start := o.bucket(startTime.Unix())
	for i := start; i < endTime.Unix()*1000; i += int64(o.bucketSize/time.Second) * 1000 {
		buckets = append(buckets, i)
	}

	return buckets
}

func (o *multiFlakeSeriesT) bucket(secs int64) int64 {
	return (secs - secs%int64(o.bucketSize/time.Second)) * 1000
}
