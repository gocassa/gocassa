gocassa
=======

Gocassa is a high-level library on top of [gocql](https://github.com/gocql/gocql).

Compared to gocql it provides query building, adds data binding, and provides easy-to-use "recipe" tables for common query use-cases. Unlike [cqlc](https://github.com/relops/cqlc), it does not use code generation. Instead, it encourages the user to define types in the most natural way for them.

#### Table types

##### Raw CQL Table

The raw CQL table pretty much lets you write any CQL query. Here is an example:

```go
package main

import(
    "fmt"
    "time"
    
    "github.com/hailocab/gocassa"
)

type Sale struct {
    Id          string
    CustomerId  string
    SellerId    string
    Price       int
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
    result := &Sale{}
    if err := sales.Where(Eq("Id", "sale-1")).Query().ReadOne(result); err != nil {
        panic(err)
    }
    fmt.Println(*result)
}
```

##### `OneToOneTable`

`OneToOneTable` provides only very simple [CRUD](http://en.wikipedia.org/wiki/Create,_read,_update_and_delete) functionality:

```go
    sales := keySpace.OneToOneTable("sale", "Id", Sale{})
    // …
    result := &Sale{}
    err := sales.Read("sale-1", result)
}
```

##### `OneToManyTable`

`OneToManyTable` can list rows filtered by equality of a single field (eg. list sales based on their `sellerId`):

```go
    saleTables := keySpace.OneToManyTable("sale", "SellerId", "Id", Sale{})
    // …
    results := &[]Sale{}
    err := sales.List("seller-1", nil, 0, results)
```

##### `TimeSeriesTable`

`TimeSeriesTable` provides an interface to list rows within a time interval:

```go
    salesTable := keySpace.TimeSeriesTable("sale", "Created", "Id", Sale{})
    //...
    results := &[]Sale{}
    err := sales.List(yesterdayTime, todayTime, results)
```

##### `TimeSeriesBTable`

`TimeSeriesBTable` is like a cross between `OneToManyTable` and `TimeSeriesTable`. It can list rows within a time interval, and filtered by equality of a single field. The following lists sales in a time interval, by a certain seller:

```go
    salesTable := keySpace.TimeSeriesBTable("sale", "SellerId", "Created", "Id", Sale{})
    //...
    results := &[]Sale{}
    err := sales.List("seller-1", yesterdayTime, todayTime, results)
```
