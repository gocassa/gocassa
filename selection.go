package cmagic

const (
	Equality	 = iota 
	GreaterThan
	GreaterThanOrEquals
	LesserThanOrEquals

)

type Relation struct {
	Op 
}

func Eq(term interface{}) Relation {

}

func In(term interface{}) Relation {

}