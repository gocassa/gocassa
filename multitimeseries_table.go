package gocassa

import (
	"fmt"
	"strings"
	"time"
)

type Bucketer interface {
	Bucket(int64) int64
	Next(int64) int64
	String() string
}

type multiTimeSeriesT struct {
	Table
	indexFields []string
	timeField   string
	idField     string
	bucketSize  time.Duration
	bucketer    Bucketer
}

type tsBucketer struct {
	bucketSize time.Duration
}

func (b *tsBucketer) Bucket(secs int64) int64 {
	return (secs - secs%int64(b.bucketSize/time.Second)) * 1000
}

func (b *tsBucketer) Next(secs int64) int64 {
	return secs + int64(b.bucketSize/time.Second)*1000
}

func (b *tsBucketer) String() string {
	return b.bucketSize.String()
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
	return o.Table.Set(m)
}

func (o *multiTimeSeriesT) bucket(secs int64) int64 {
	return o.bucketer.Bucket(secs)
}

func (o *multiTimeSeriesT) Update(v interface{}, timeStamp time.Time, id interface{}, m map[string]interface{}) Op {
	bucket := o.bucket(timeStamp.Unix())
	idxs, err := o.indexes(v)
	if err != nil {
		return &badOp{err}
	}
	relations := fRelations(idxs, Eq(bucketFieldName, bucket), Eq(o.timeField, timeStamp), Eq(o.idField, id))
	return o.Where(relations...).Update(m)
}

func (o *multiTimeSeriesT) Delete(v interface{}, timeStamp time.Time, id interface{}) Op {
	bucket := o.bucket(timeStamp.Unix())
	idxs, err := o.indexes(v)
	if err != nil {
		return &badOp{err}
	}
	relations := fRelations(idxs, Eq(bucketFieldName, bucket), Eq(o.timeField, timeStamp), Eq(o.idField, id))
	return o.Where(relations...).Delete()
}

func (o *multiTimeSeriesT) Read(v interface{}, timeStamp time.Time, id, pointer interface{}) Op {
	bucket := o.bucket(timeStamp.Unix())
	idxs, err := o.indexes(v)
	if err != nil {
		return &badOp{err}
	}
	relations := fRelations(idxs, Eq(bucketFieldName, bucket), Eq(o.timeField, timeStamp), Eq(o.idField, id))
	return o.Where(relations...).ReadOne(pointer)
}

func (o *multiTimeSeriesT) List(v interface{}, startTime time.Time, endTime time.Time, pointerToASlice interface{}) Op {
	buckets := []interface{}{}
	start := o.bucket(startTime.Unix())
	end := o.bucket(endTime.Unix())
	for i := start; i <= end; i = o.bucketer.Next(i) { // nearly but not quite an iterator
		buckets = append(buckets, i)
	}
	idxs, err := o.indexes(v)
	if err != nil {
		return &badOp{err}
	}
	relations := fRelations(idxs, In(bucketFieldName, buckets...), GTE(o.timeField, startTime), LTE(o.timeField, endTime))
	return o.Where(relations...).Read(pointerToASlice)
}

func (o *multiTimeSeriesT) WithOptions(opt Options) MultiTimeSeriesTable {
	return &multiTimeSeriesT{
		Table:       o.Table.WithOptions(opt),
		indexFields: o.indexFields,
		timeField:   o.timeField,
		idField:     o.idField,
		bucketer:    o.bucketer,
	}
}

func BucketerString(b Bucketer) string {
	n := fmt.Sprintf("%T", b)
	split := strings.Split(n, ".")
	return split[len(split)-1]
}

// indexes takes the supplied index value or values, if passed as map[string]interface{}, and takes those that match the
// configured indexFields to use as Eq(...) Relation objects havin checked that we have exactly enough to
// specify all the partition keys.
func (o *multiTimeSeriesT) indexes(iv interface{}) ([]Relation, error) {
	var indexes []Relation
	v, err := o.indexesAsMap(iv)
	if err != nil {
		return nil, err
	}
	ni := 0
	for _, i := range o.indexFields {
		if vv, ok := v[i]; ok {
			indexes = append(indexes, Eq(i, vv))
			ni++
		}
	}
	if ni != len(o.indexFields) {
		return nil, fmt.Errorf("Indexes incomplete: %+v", o.indexFields)
	}
	return indexes, nil
}

// indexesAsMap returns the indexes as a map[string]interface{}: if supplied with a map[string]interface{} simply passes this through
// otherwise takes the single value and wraps in a map[string]interface{} having
// checked that no more than one partition key is configured.
func (o *multiTimeSeriesT) indexesAsMap(v interface{}) (map[string]interface{}, error) {
	switch vt := v.(type) {
	case map[string]interface{}:
		return vt, nil
	default:
		if len(o.indexFields) != 1 {
			return nil, fmt.Errorf("Must pass map of values if more than one indexField (have: %d): got: %+v", len(o.indexFields), v)
		}
		return map[string]interface{}{o.indexFields[0]: v}, nil
	}
}

func fRelations(i []Relation, o ...Relation) []Relation {
	return append(i, o...)
}
