package gocassa

import (
	"time"
)

type bucketIter struct {
	v         time.Time
	step      time.Duration
	field     string
	invariant filter
}

func (b bucketIter) String() string {
	return b.v.String()
}

func (b bucketIter) Bucket() time.Time {
	step := b.step
	if step < time.Second {
		step = time.Second
	}
	secs := b.v.Unix()
	return time.Unix((secs - secs%int64(step/time.Second)), 0)
}

func (b bucketIter) Next() Buckets {
	return bucketIter{
		v:         b.v.Add(b.step),
		step:      b.step,
		invariant: b.invariant,
		field:     b.field}
}

func (b bucketIter) Prev() Buckets {
	return bucketIter{
		v:         b.v.Add(-b.step),
		step:      b.step,
		invariant: b.invariant,
		field:     b.field}
}

func (b bucketIter) Filter() Filter {
	rels := b.invariant.rs
	rels = append(rels, Eq(b.field, b.Bucket()))
	return b.invariant.t.Where(rels...)
}
