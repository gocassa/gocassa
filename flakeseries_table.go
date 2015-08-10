package gocassa

import (
	"errors"
	"strings"
	"time"

	"github.com/mattheath/base62"
	"github.com/mattheath/kala/bigflake"
	"github.com/mattheath/kala/util"
)

// we have to put the timestamp from the flake ID into a field so that we can
// execute queries with it
const flakeTimestampFieldName = "flake_created"

type flakeSeriesT struct {
	Table
	bucketSize time.Duration
}

func (o *flakeSeriesT) Set(v interface{}) (Op, error) {
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

func (o *flakeSeriesT) Update(id string, m map[string]interface{}) (Op, error) {
	timestamp, err := FlakeToTime(id)
	if err != nil {
		return nil, err
	}
	bucket := o.bucket(timestamp.Unix())

	return o.Where(Eq(bucketFieldName, bucket), Eq(flakeTimestampFieldName, timestamp), Eq("Id", id)).Update(m), nil
}

func (o *flakeSeriesT) Delete(id string) (Op, error) {
	timestamp, err := FlakeToTime(id)
	if err != nil {
		return nil, err
	}
	bucket := o.bucket(timestamp.Unix())

	return o.Where(Eq(bucketFieldName, bucket), Eq(flakeTimestampFieldName, timestamp), Eq("Id", id)).Delete(), nil
}

func (o *flakeSeriesT) Read(id string, pointer interface{}) (Op, error) {
	timestamp, err := FlakeToTime(id)
	if err != nil {
		return nil, err
	}
	bucket := o.bucket(timestamp.Unix())
	return o.Where(Eq(bucketFieldName, bucket), Eq(flakeTimestampFieldName, timestamp), Eq("Id", id)).ReadOne(pointer), nil
}

func (o *flakeSeriesT) List(startTime, endTime time.Time, pointerToASlice interface{}) Op {
	buckets := o.buckets(startTime, endTime)
	return o.Where(In(bucketFieldName, buckets...), GTE(flakeTimestampFieldName, startTime), LT(flakeTimestampFieldName, endTime)).Read(pointerToASlice)
}

// ListSince queries the flakeSeries for the items after the specified ID but within the time window,
// if the time window is zero then it lists up until 5 minutes in the future
func (o *flakeSeriesT) ListSince(id string, window time.Duration, pointerToASlice interface{}) (Op, error) {
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
	return o.Where(In(bucketFieldName, buckets), GTE(flakeTimestampFieldName, startTime), LT(flakeTimestampFieldName, endTime), GT("Id", id)).Read(pointerToASlice), nil
}

func (o *flakeSeriesT) WithOptions(opt Options) FlakeSeriesTable {
	return &flakeSeriesT{
		Table:      o.Table.WithOptions(opt),
		bucketSize: o.bucketSize,
	}
}

func (o *flakeSeriesT) buckets(startTime, endTime time.Time) []interface{} {
	buckets := []interface{}{}
	start := o.bucket(startTime.Unix())
	for i := start; i < endTime.Unix()*1000; i += int64(o.bucketSize/time.Second) * 1000 {
		buckets = append(buckets, i)
	}

	return buckets
}

func (o *flakeSeriesT) bucket(secs int64) int64 {
	return (secs - secs%int64(o.bucketSize/time.Second)) * 1000
}

func FlakeToTime(id string) (time.Time, error) {
	parts := strings.Split(id, "_")

	if len(parts) < 2 {
		return time.Time{}, errors.New("Invalid flake id")
	}

	intId := base62.DecodeToBigInt(parts[len(parts)-1])

	msTime, _, _ := bigflake.ParseId(intId)
	timestamp := util.MsInt64ToTime(msTime)

	return timestamp, nil
}
