gocassa
=======

Gocassa is a high-level library on top of [gocql](https://github.com/gocql/gocql).

Compared to gocql it provides query building, adds data binding, and provides easy-to-use "recipe" tables for common query use-cases. Unlike [cqlc](https://github.com/relops/cqlc), it does not use code generation.

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
    salesTable := keySpace.Table("sale", Sale{}, gocassa.Keys{
        PartitionKeys: []string{"Id"},
    })
    err = salesTable.Set(Sale{
        Id: "sale-1",
        CustomerId: "customer-1",
        SellerId: "seller-1",
        Price: 42,
        Created: time.Now(),
    }).Run()
    if err != nil {
        panic(err)
    }
    result := Sale{}
    if err := salesTable.Where(gocassa.Eq("Id", "sale-1")).Query().ReadOne(&result).Run(); err != nil {
        panic(err)
    }
    fmt.Println(result)
}
```

The following 'recipe' tables were designed to "guide" the user in terms of what queries can they make on a certain partition key - clustering column combination:

##### `MapTable`

`MapTable` provides only very simple [CRUD](http://en.wikipedia.org/wiki/Create,_read,_update_and_delete) functionality:

```go
    salesTable := keySpace.MapTable("sale", "Id", Sale{})
    // …
    result := Sale{}
    err := salesTable.Read("sale-1", &result).Run()
}
```

##### `MultimapTable`

`MultimapTable` can list rows filtered by equality of a single field (eg. list sales based on their `sellerId`):

```go
    salesTable := keySpace.MultimapTable("sale", "SellerId", "Id", Sale{})
    // …
    results := []Sale{}
    err := salesTable.List("seller-1", nil, 0, &results).Run()
```

##### `TimeSeriesTable`

`TimeSeriesTable` provides an interface to list rows within a time interval:

```go
    salesTable := keySpace.TimeSeriesTable("sale", "Created", "Id", Sale{})
    //...
    results := []Sale{}
    err := salesTable.List(yesterdayTime, todayTime, &results).Run()
```

##### `MultiTimeSeriesTable`

`MultiTimeSeriesTable` is like a cross between `MultimapTable` and `TimeSeriesTable`. It can list rows within a time interval, and filtered by equality of a single field. The following lists sales in a time interval, by a certain seller:

```go
    salesTable := keySpace.MultiTimeSeriesTable("sale", "SellerId", "Created", "Id", Sale{})
    //...
    results := []Sale{}
    err := salesTable.List("seller-1", yesterdayTime, todayTime, &results).Run()
```
