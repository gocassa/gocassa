package gocassa

import (
	"time"
)

type multiFlakeSeriesT struct {
	Table
	indexField string
	bucketSize time.Duration
}

func (o *multiFlakeSeriesT) Set(v interface{}) (Op, error) {
	m, ok := toMap(v)
	if !ok {
		panic("Can't set: not able to convert")
	}
	id, ok := m["Id"].(string)
	if !ok {
		panic("Id field is not present or is not a string")
	}

	timestamp, err := FlakeToTime(id)

	if err != nil {
		return nil, err
	}

	m[flakeTimestampFieldName] = timestamp
	m[bucketFieldName] = o.bucket(timestamp.Unix())

	return o.Table.Set(m), nil
}

func (o *multiFlakeSeriesT) Update(v interface{}, id string, m map[string]interface{}) (Op, error) {
	timestamp, err := FlakeToTime(id)
	if err != nil {
		return nil, err
	}
	bucket := o.bucket(timestamp.Unix())

	return o.Where(Eq(o.indexField, v), Eq(bucketFieldName, bucket), Eq(flakeTimestampFieldName, timestamp), Eq("Id", id)).Update(m), nil
}

func (o *multiFlakeSeriesT) Delete(v interface{}, id string) (Op, error) {
	timestamp, err := FlakeToTime(id)
	if err != nil {
		return nil, err
	}
	bucket := o.bucket(timestamp.Unix())

	return o.Where(Eq(o.indexField, v), Eq(bucketFieldName, bucket), Eq(flakeTimestampFieldName, timestamp), Eq("Id", id)).Delete(), nil
}

func (o *multiFlakeSeriesT) Read(v interface{}, id string, pointer interface{}) (Op, error) {
	timestamp, err := FlakeToTime(id)
	if err != nil {
		return nil, err
	}
	bucket := o.bucket(timestamp.Unix())
	return o.Where(Eq(o.indexField, v), Eq(bucketFieldName, bucket), Eq(flakeTimestampFieldName, timestamp), Eq("Id", id)).ReadOne(pointer), nil
}

func (o *multiFlakeSeriesT) List(v interface{}, startTime, endTime time.Time, pointerToASlice interface{}) (Op, error) {
	buckets := o.buckets(startTime, endTime)
	return o.Where(Eq(o.indexField, v), In(bucketFieldName, buckets...), GTE(flakeTimestampFieldName, startTime), LT(flakeTimestampFieldName, endTime)).Read(pointerToASlice), nil
}

// ListSince queries the flakeSeries for the items after the specified ID but within the time window,
// if the time window is zero then it lists up until 5 minutes in the future
func (o *multiFlakeSeriesT) ListSince(v interface{}, id string, window time.Duration, pointerToASlice interface{}) (Op, error) {
	startTime, err := FlakeToTime(id)
	if err != nil {
		return nil, err
	}

	var endTime time.Time

	if window == 0 {
		// no window set - so go up until 5 mins in the future
		endTime = time.Now().Add(5 * time.Minute)
	} else {
		endTime = startTime.Add(window)
	}
	buckets := o.buckets(startTime, endTime)
	return o.Where(Eq(o.indexField, v), In(bucketFieldName, buckets), GTE(flakeTimestampFieldName, startTime), LT(flakeTimestampFieldName, endTime), GT("Id", id)).Read(pointerToASlice), nil
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
