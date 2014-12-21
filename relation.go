package cmagic

import(
	//"strings"
)

const (
	equality	 = iota
	in
	greaterThan
	greaterThanOrEquals
	lesserThan
	lesserThanOrEquals
)

type Relation struct {
	op 	  int
	key   string
	terms []interface{}
}

func (r Relation) cql() (string, []interface{}) {
	ret := ""
	switch r.op {
	case equality:
		ret = r.key + " = ?"
	case in:
		return r.key + " IN (?)", []interface{}{r.terms}
	case greaterThan:
		ret = r.key + " > ?"
	case greaterThanOrEquals:
		ret = r.key + " >= ?"
	case lesserThan:
		ret = r.key + " < ?"
	case lesserThanOrEquals:
		ret = r.key + " <= ?"
	}
	return ret, r.terms
}

func toI(i interface{}) []interface{} {
	return []interface{}{i}
}

func Eq(key string, term interface{}) Relation {
	return Relation{
		op: equality,
		key: key,
		terms: toI(term),
	}
}

// In is broken at the moment. Please don't use
func In(key string, terms ...interface{}) Relation {
	return Relation{
		op: in,
		key: key,
		terms: terms,
	}
}

func GT(key string, term interface{}) Relation {
	return Relation{
		op: greaterThan,
		key: key,
		terms: toI(term),
	}
}

func GTE(key string, term interface{}) Relation {
	return Relation{
		op: greaterThanOrEquals,
		key: key,
		terms: toI(term),
	}
}

func LT(key string, term interface{}) Relation {
	return Relation{
		op: lesserThan,
		key: key,
		terms: toI(term),
	}
}

func LTE(key string, term interface{}) Relation {
	return Relation{
		op: lesserThanOrEquals,
		key: key,
		terms: toI(term),
	}
}