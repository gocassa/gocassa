gocassa
===

Gocassa is a high level library on top of [gocql](https://github.com/gocql/gocql).

Compared to gocql, it provides query building, data binding and it identifies certain query use cases and provides different kind of tables for them. Unlike [cqlc](https://github.com/relops/cqlc), it does not require the user to generate code. It encourages the user to define their types in a way which is most natural for them.

#### Table types

##### Raw CQL Table

The Raw CQL table pretty much let's you write any CQL query, here is an example:

```go
package main

import(
	"time"
	"github.com/hailocab/gocassa"
	"fmt"
)

type Sale struct {
	Id 			string
	CustomerId	string
	SellerId 	string
	Price 		int
	Created     time.Time
}

func main() {
	keySpace, err := gocassa.ConnectToKeySpace("test", []string{"127.0.0.1"}, "", "")
	if err != nil {
		panic(err)
	}
	sales := keySpace.Table("sale", Sale{}, gocassa.Keys{
		PartitionKeys: []string{"Id"},
	})
	err = sales.Set(Sale{
		Id: "sale-1",
		CustomerId: "customer-1",
		SellerId: "seller-1",
		Price: 42,
		Created: time.Now(),
	})
	if err != nil {
		panic(err)
	}
	sale, err := sales.Where(Eq("Id", "sale-1")).Query().Read()
	if err != nil {
		panic(err)
	}
	fmt.Println(sale.(*Sale))
}
```

##### OneToOne Table

OneToOne table provides only very simple CRUD functionality.

```go
	sales := keySpace.OneToOneTable("sale", "Id", Sale{})
	// ...
	sale, err := sales.Read("sale-1")
}
```

##### OneToMany Table

OneToMany can list rows based on a field equality, eg. if we want to list sales based on sellerId:

```go
	saleTables := keySpace.OneToOneTable("sale", "SellerId", Id", Sale{})
	//...
	sale, err := sales.List("seller-1", nil, nil)
```

##### TimeSeries Table

TimeSeries provides an interface to list rows between two time ranges.

```go
	salesTable := keySpace.TimeSeriesTable("sale", "Created", "Id", Sale{})
	//...
	sales, err := sales.List(yesterdayTime, todayTime)
```

##### TimeSeriesB Table

TimeSeriesB is like a cross between OneToMany and TimeSeries, it lets you list rows between time ranges, filtered by a field equality.
The following lists sales between two time ranges, done by a certain seller:

```go
	salesTable := keySpace.TimeSeriesBTable("sale", "SellerId", "Created", "Id", Sale{})
	//...
	sales, err := sales.List("seller-1", yesterdayTime, todayTime)
```
