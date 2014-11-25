package cmagic

// First sketch, trying to keep this simple... simpler than erdos... so I have to change the interface, which means it is in flux.

type NameSpace interface {
	Collection() Collection
}

type Collection interface {
	Read(id string) Record // Load record immediately - we need to return error, or lazy load it, which means we need to return errors at later operations
	MultiRead(ids []string) ([]Record, errors)
	List(idStart, idEnd string, limit int) ([]Record, errors)
	Set(id string, interface{}) error
}

// A record loaded from a database
type Record interface {
	Set(interface{}) error
	ToMap() map[string]interface{}
	ToStruct(i interface{})
}