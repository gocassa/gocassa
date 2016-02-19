package gocassa

import (
	"time"
	"strings"
)

type Bucketer interface {
	Bucket(int64) int64
	Next(int64) int64
}

type flexTimeSeriesT struct {
	Table
	indexFields []string
	timeField  string
	idField    string
	bucketer   Bucketer
}

func (o *flexTimeSeriesT) Set(v interface{}) Op {
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

func (o *flexTimeSeriesT) bucket(secs int64) int64 {
	return o.bucketer.Bucket(secs)
}

func (o *flexTimeSeriesT) Update(v map[string]interface{}, timeStamp time.Time, id interface{}, m map[string]interface{}) Op {
	bucket := o.bucket(timeStamp.Unix())
	relations := fRelations(o.indexes(v), Eq(bucketFieldName, bucket), Eq(o.timeField, timeStamp), Eq(o.idField, id))
	return o.Where(relations...).Update(m)
}

func (o *flexTimeSeriesT) Delete(v map[string]interface{}, timeStamp time.Time, id interface{}) Op {
	bucket := o.bucket(timeStamp.Unix())
	relations := fRelations(o.indexes(v), Eq(bucketFieldName, bucket), Eq(o.timeField, timeStamp), Eq(o.idField, id))
	return o.Where(relations...).Delete()
}

func (o *flexTimeSeriesT) Read(v map[string]interface{}, timeStamp time.Time, id, pointer interface{}) Op {
	bucket := o.bucket(timeStamp.Unix())
	relations := fRelations(o.indexes(v), Eq(bucketFieldName, bucket), Eq(o.timeField, timeStamp), Eq(o.idField, id))
	return o.Where(relations...).ReadOne(pointer)
}

func (o *flexTimeSeriesT) List(v map[string]interface{}, startTime time.Time, endTime time.Time, pointerToASlice interface{}) Op {
	buckets := []interface{}{}
	start := o.bucket(startTime.Unix())
	end := o.bucket(endTime.Unix())
	for i := start; i <= end; i = o.bucketer.Next(i) { // nearly but not quite an iterator
		buckets = append(buckets, i)
	}
	relations := fRelations(o.indexes(v), In(bucketFieldName, buckets...), GTE(o.timeField, startTime), LTE(o.timeField, endTime))
	return o.Where(relations...).Read(pointerToASlice)
}

func (o *flexTimeSeriesT) WithOptions(opt Options) FlexTimeSeriesTable {
	return &flexTimeSeriesT{
		Table:      o.Table.WithOptions(opt),
		indexFields: o.indexFields,
		timeField:  o.timeField,
		idField:    o.idField,
		bucketer:   o.bucketer,
	}
}

func (o *flexTimeSeriesT) indexes(v map[string]interface{}) []Relation {
	var indexes []Relation
	ni := 0
	for _, i := range o.indexFields {
		if vv, ok := v[i]; ok {
			indexes = append(indexes, Eq(i, vv))
			ni++
		}
	}
	if ni != len(o.indexFields) {
		panic("Indexes incomplete: "+strings.Join(o.indexFields, ","))
	}
	return indexes
}

func fRelations(i []Relation, o... Relation) []Relation {
	return append(i, o...)
}
//func (o *flexTimeSeriesT) indexes(v map[string]interface{}) []Relation {
//	return matchFields(o.indexFields, v, true)
//}
//
//func (o *flexTimeSeriesT) allIds(v map[string]interface{}) []Relation {
//	return matchFields(o.idFields, v, true)
//}
//
//func (o *flexTimeSeriesT) partIds(v map[string]interface{}) []Relation {
//	return matchFields(o.idFields, v, false)
//}
//
//func matchFields(fields []string, v map[string]interface{}, all bool) []Relation {
//	var ops []Relation
//	ni := 0
//	for _, i := range fields {
//		if vv, ok := v[i]; ok {
//			ops = append(ops, Eq(i, vv))
//			ni++
//		}
//	}
//	if all && ni != len(fields) {
//		panic("Indexes incomplete: "+strings.Join(fields, ","))
//	}
//	return ops
//}