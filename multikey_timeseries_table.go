package gocassa

import (
	"time"
)

type multiKeyTimeSeriesT struct {
	t           Table
	indexFields []string
	timeField   string
	idFields    []string
	bucketSize  time.Duration
}

func (o *multiKeyTimeSeriesT) Table() Table                        { return o.t }
func (o *multiKeyTimeSeriesT) Create() error                       { return o.Table().Create() }
func (o *multiKeyTimeSeriesT) CreateIfNotExist() error             { return o.Table().CreateIfNotExist() }
func (o *multiKeyTimeSeriesT) Name() string                        { return o.Table().Name() }
func (o *multiKeyTimeSeriesT) Recreate() error                     { return o.Table().Recreate() }
func (o *multiKeyTimeSeriesT) CreateStatement() (Statement, error) { return o.Table().CreateStatement() }
func (o *multiKeyTimeSeriesT) CreateIfNotExistStatement() (Statement, error) {
	return o.Table().CreateIfNotExistStatement()
}

func (o *multiKeyTimeSeriesT) Set(v interface{}) Op {
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

func (o *multiKeyTimeSeriesT) Update(v map[string]interface{}, timeStamp time.Time, id map[string]interface{}, m map[string]interface{}) Op {
	bucket := bucket(timeStamp, o.bucketSize)
	relations := make([]Relation, 0)
	relations = append(relations, o.ListOfEqualRelations(v, id)...)
	relations = append(relations, Eq(bucketFieldName, bucket))
	relations = append(relations, Eq(o.timeField, timeStamp))

	return o.Table().
		Where(relations...).
		Update(m)
}

func (o *multiKeyTimeSeriesT) Delete(v map[string]interface{}, timeStamp time.Time, id map[string]interface{}) Op {
	bucket := bucket(timeStamp, o.bucketSize)
	relations := make([]Relation, 0)
	relations = append(relations, o.ListOfEqualRelations(v, id)...)
	relations = append(relations, Eq(bucketFieldName, bucket))
	relations = append(relations, Eq(o.timeField, timeStamp))

	return o.Table().
		Where(relations...).
		Delete()
}

func (o *multiKeyTimeSeriesT) Read(v map[string]interface{}, timeStamp time.Time, id map[string]interface{}, pointer interface{}) Op {
	bucket := bucket(timeStamp, o.bucketSize)
	relations := make([]Relation, 0)
	relations = append(relations, o.ListOfEqualRelations(v, id)...)
	relations = append(relations, Eq(bucketFieldName, bucket))
	relations = append(relations, Eq(o.timeField, timeStamp))

	return o.Table().
		Where(relations...).
		ReadOne(pointer)

}

func (o *multiKeyTimeSeriesT) List(v map[string]interface{}, startTime time.Time, endTime time.Time, pointerToASlice interface{}) Op {
	buckets := []interface{}{}
	for bucket := o.Buckets(v, startTime); bucket.Bucket().Before(endTime); bucket = bucket.Next() {
		buckets = append(buckets, bucket.Bucket())
	}

	relations := make([]Relation, 0)
	relations = append(relations, o.ListOfEqualRelations(v, nil)...)
	relations = append(relations, In(bucketFieldName, buckets...))
	relations = append(relations, GTE(o.timeField, startTime))
	relations = append(relations, LTE(o.timeField, endTime))

	return o.Table().
		Where(relations...).
		Read(pointerToASlice)
}

func (o *multiKeyTimeSeriesT) Buckets(v map[string]interface{}, start time.Time) Buckets {
	return bucketIter{
		v:         start,
		step:      o.bucketSize,
		field:     bucketFieldName,
		invariant: o.Table().Where(o.ListOfEqualRelations(v, nil)...)}
}

func (o *multiKeyTimeSeriesT) WithOptions(opt Options) MultiKeyTimeSeriesTable {
	return &multiKeyTimeSeriesT{
		t:           o.Table().WithOptions(opt),
		indexFields: o.indexFields,
		timeField:   o.timeField,
		idFields:    o.idFields,
		bucketSize:  o.bucketSize,
	}
}

func (o *multiKeyTimeSeriesT) ListOfEqualRelations(fieldsToIndex, ids map[string]interface{}) []Relation {
	relations := make([]Relation, 0)

	for _, field := range o.indexFields {
		if value := fieldsToIndex[field]; value != nil && value != "" {
			relation := Eq(field, value)
			relations = append(relations, relation)
		}
	}

	for _, field := range o.idFields {
		if value := ids[field]; value != nil && value != "" {
			relation := Eq(field, value)
			relations = append(relations, relation)
		}
	}

	return relations
}
