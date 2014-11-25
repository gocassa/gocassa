package cmagic

// First sketch, trying to keep this simple... simpler than erdos... so I have to change the interface, which means it is in flux.

type NameSpace interface {
	Collection(name string, entity interface{}) Collection
}

type Collection interface {
	Read(id string) Record // Load record immediately - we need to return error, or lazy load it, which means we need to return errors at later operations
	MultiRead(ids []string) ([]Record, errors)
	List(idStart, idEnd string, limit int) ([]Record, errors)
	Set(id string, interface{}) error // Use for creating a record @cruftalert
}

// A record loaded from a database
type Record interface {
	Set(interface{}) // Used for updating a loaded record @cruftalert
	ToMap() map[string]interface{}
	ToStruct(i interface{})
}

// These are just here to not forget about them

type TimeSeriesIndex interface {

}

type EqualityIndex interface {

}