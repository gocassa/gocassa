package cmagic

// First sketch, trying to keep this simple...

type NameSpace interface {
	Collection() Collection
}

type Collection interface {
	Record(id string) Record
}

type Record interface {
	Set(i interface{}) error
	ToMap() map[string]interface{}
	ToStruct(i interface{})
}