gocassa
===

Gocassa is a high-level library on top of [gocql](https://github.com/gocql/gocql).

Compared to gocql, it provides query building, data binding, and it identifies certain query use cases and provides different kinds of tables for them. Unlike [cqlc](https://github.com/relops/cqlc), it does not require the user to generate code. It encourages the user to define their types in the most natural way for them.

#### Table types

##### Raw CQL Table

The Raw CQL table pretty much lets you write any CQL query. Here is an example:

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

##### `OneToOneTable`

`OneToOneTable` provides only very simple CRUD functionality:

```go
	sales := keySpace.OneToOneTable("sale", "Id", Sale{})
	// ...
	sale, err := sales.Read("sale-1")
}
```

##### `OneToManyTable`

`OneToManyTable` can list rows based on a field equality (eg. if we want to list sales based on `sellerId`):

```go
	saleTables := keySpace.OneToOneTable("sale", "SellerId", Id", Sale{})
	//...
	sale, err := sales.List("seller-1", nil, nil)
```

##### `TimeSeriesTable`

`TimeSeriesTable` provides an interface to list rows between two time ranges:

```go
	salesTable := keySpace.TimeSeriesTable("sale", "Created", "Id", Sale{})
	//...
	sales, err := sales.List(yesterdayTime, todayTime)
```

##### `TimeSeriesBTable`

`TimeSeriesBTable` is like a cross between `OneToManyTable` and `TimeSeriesTable`. It lets you list rows between time ranges, filtered by a equality of a single field. The following lists sales between two time ranges, by a certain seller:

```go
	salesTable := keySpace.TimeSeriesBTable("sale", "SellerId", "Created", "Id", Sale{})
	//...
	sales, err := sales.List("seller-1", yesterdayTime, todayTime)
```
