package gocassa

import (
	"strings"
	"time"
)

const (
	equality = iota
	in
	greaterThan
	greaterThanOrEquals
	lesserThan
	lesserThanOrEquals
)

type Relation struct {
	op    int
	key   string
	terms []interface{}
}

func (r Relation) cql() (string, []interface{}) {
	ret := ""
	key := strings.ToLower(r.key)
	switch r.op {
	case equality:
		ret = key + " = ?"
	case in:
		return key + " IN ?", r.terms
	case greaterThan:
		ret = key + " > ?"
	case greaterThanOrEquals:
		ret = key + " >= ?"
	case lesserThan:
		ret = key + " < ?"
	case lesserThanOrEquals:
		ret = key + " <= ?"
	}
	return ret, r.terms
}

func anyEquals(value interface{}, terms []interface{}) bool {
	primVal := convertToPrimitive(value)
	for _, term := range terms {
		if primVal == convertToPrimitive(term) {
			return true
		}
	}
	return false
}

func convertToPrimitive(i interface{}) interface{} {
	switch v := i.(type) {
	case time.Time:
		return v.UnixNano()
	case time.Duration:
		return v.Nanoseconds()
	default:
		return i
	}
}

func (r Relation) accept(i interface{}) bool {
	var result bool
	var err error

	if r.op == equality || r.op == in {
		return anyEquals(i, r.terms)
	}

	a, b := convertToPrimitive(i), convertToPrimitive(r.terms[0])

	switch r.op {
	case greaterThan:
		result, err = builtinGreaterThan(a, b)
	case greaterThanOrEquals:
		result, err = builtinGreaterThan(a, b)
		result = result || a == b
	case lesserThanOrEquals:
		result, err = builtinLessThan(a, b)
		result = result || a == b
	case lesserThan:
		result, err = builtinLessThan(a, b)
	}

	return err == nil && result
}

func toI(i interface{}) []interface{} {
	return []interface{}{i}
}

func Eq(key string, term interface{}) Relation {
	return Relation{
		op:    equality,
		key:   key,
		terms: toI(term),
	}
}

func In(key string, terms ...interface{}) Relation {
	return Relation{
		op:    in,
		key:   key,
		terms: terms,
	}
}

func GT(key string, term interface{}) Relation {
	return Relation{
		op:    greaterThan,
		key:   key,
		terms: toI(term),
	}
}

func GTE(key string, term interface{}) Relation {
	return Relation{
		op:    greaterThanOrEquals,
		key:   key,
		terms: toI(term),
	}
}

func LT(key string, term interface{}) Relation {
	return Relation{
		op:    lesserThan,
		key:   key,
		terms: toI(term),
	}
}

func LTE(key string, term interface{}) Relation {
	return Relation{
		op:    lesserThanOrEquals,
		key:   key,
		terms: toI(term),
	}
}
