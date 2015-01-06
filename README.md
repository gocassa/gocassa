SUCH MAGIC, MUCH CASSANDRA WOW
===

This library is highly experimental.


### Example: cassa geoindex

The following thing:

```
CREATE TABLE geo_index (
  geohash text,
  realm text,
  unique_id int,
  update_time timestamp,
  lat float,
  lon float,
  PRIMARY KEY (geohash, realm, unique_id)
)
```

Would be represented in Go like:

```go
type GeoIndex struct {
	Geohash string
	Realm string
	UniqueId int
	UpdateTime time.Time
	Lat float64
	Long float64
}
keys := Keys{
	PartitionKeys: []string{"Geohash", "UniqueId"},
}
geoTable := keyspace.Table("GeoIndex", GeoIndex{}, keys)
```

Then

```go
queryString := fmt.Sprintf("INSERT INTO %v (geohash, realm, unique_id, update_time, lat, lon) VALUES ('%s', '%s', %d, dateof(now()), %f, %f) USING TTL %d;", table, geoHashes["centre"], realm, uniqueId, latitude, longitude, ttl)

// Equals to

g := GeoIndex{
	GeoHash: "ff8989x",
	Realm: "London",
	UniqueId: 42,
	Lat: 0.1,
	Long: 0.2
}
// Note: TTL is missing, Insert needs no selection? There are problems with this...
geoTable.Insert(g)
```

Querying:

```go
queryString := fmt.Sprintf("SELECT geohash, realm, unique_id, lat, lon, update_time FROM %v WHERE geohash IN (%v) AND realm = '%v';", table, geoHashesList, realm)

// Equals to
geoHashesList := []string{"absdsd3", "fddff833f", "hsbrh3g4h3", "j3hg43h4g3hg4"}
rows, err := geoTable.Where(In("geoHash", geoHashesList...)).Query().Read()
```

### Recipes are in progress for this library:

The idea behind Recipes is that we identify certain query patterns and instead of letting people define their own PartitionKeys and Clustering Columns and then being able to construct any kind of query,
we:
- name certain PartitionKey-ClusteringColumn patterns: (("Id")) becomes Entity, ((SomeField), Id) becomes OneToMany
- creating different table objects for the different recipes
- restrict the queries the can make on such table object thus not allowing invalid queries to be made on a table

This will hopefully decrease the cognitive load when working with the most common usecases and increase the level of type safety somewhat to prevent runtime errors.

Something like this vague sketch here:

```
// New entity recipe
func NewEntity(interface{}) Entity {
	//...
}

type Entity interface {
	//Crud stuff:
	Read(id string) (interface{}, error)
	Set(i interface{}) error
	Delete(id string) error
}

// fieldName is the field to do the query based on
func NewOneToMany(fieldName string, interface{}) OneToMany {
}

type OneToMany interface {
	//CRUD stuff (still in progress since it can not be the same as)
	List(fieldEqualsTo interface{}, p PagingOptions) ([]interface, error)
}
```