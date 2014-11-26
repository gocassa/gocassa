package cmagic

// This stuff is in flux.

type NameSpace interface {
	Collection(name string, entity interface{}) Collection
}

type Collection interface {
	Read(id string) (interface{}, error) // Read(id string, v interface{})???, that is more conventional in Go land
	// Just have a set method? How would that play with CQL?
	Create(v interface{}) error
	Update(v interface{}) error
	Delete(id string) error
	//MultiRead(ids []string) ([]interface{}, error)
	//List(idStart, idEnd string, limit int) ([]interface{}, error)
}

// These are just here to not forget about them

type TimeSeriesIndex interface {

}

type EqualityIndex interface {
	//Equals(key string, value interface{}, idStart, endEnd, limit int)
}