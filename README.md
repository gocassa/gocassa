gocassa
=======

[![GoDoc](https://img.shields.io/badge/godoc-reference-blue.svg "GoDoc")](http://godoc.org/github.com/mondough/gocassa) 
[![Build Status](https://img.shields.io/travis/mondough/gocassa/master.svg "Build Status")](https://travis-ci.org/mondough/gocassa) 

Gocassa is a high-level library on top of [gocql](https://github.com/gocql/gocql).

Compared to gocql it provides query building, adds data binding, and provides easy-to-use "recipe" tables for common query use-cases. Unlike [cqlc](https://github.com/relops/cqlc), it does not use code generation.

For docs, see: [https://godoc.org/github.com/mondough/gocassa](https://godoc.org/github.com/mondough/gocassa)

#### Table types

Gocassa provides multiple table types with their own unique interfaces:
- a raw CQL table called simply `Table` - this lets you do pretty much any query imaginable
- and a number of single purpose 'recipe' tables (`Map`, `Multimap`, `TimeSeries`, `MultiTimeSeries`), which aims to help the user by having a simplified interface tailored to a given common query use case

##### `Table`

```go
package main

import(
    "fmt"
    "time"
    
    "github.com/mondough/gocassa"
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
    if err := salesTable.Where(gocassa.Eq("Id", "sale-1")).ReadOne(&result).Run(); err != nil {
        panic(err)
    }
    fmt.Println(result)
}
```
[link to this example](https://github.com/mondough/gocassa/blob/master/examples/table1/table1.go)

##### `MapTable`

`MapTable` provides only very simple [CRUD](http://en.wikipedia.org/wiki/Create,_read,_update_and_delete) functionality:

```go
    // …
    salesTable := keySpace.MapTable("sale", "Id", Sale{})
    result := Sale{}
    salesTable.Read("sale-1", &result).Run()
}
```
[link to this example](https://github.com/mondough/gocassa/blob/master/examples/map_table1/map_table1.go)

Read, Set, Update, and Delete all happen by "Id".

##### `MultimapTable`

`MultimapTable` can list rows filtered by equality of a single field (eg. list sales based on their `sellerId`):

```go
    salesTable := keySpace.MultimapTable("sale", "SellerId", "Id", Sale{})
    // …
    results := []Sale{}
    err := salesTable.List("seller-1", nil, 0, &results).Run()
```
[link to this example](https://github.com/mondough/gocassa/blob/master/examples/multimap_table1/multimap_table1.go)

For examples on how to do pagination or Update with this table, refer to the example (linked under code snippet). 

##### `TimeSeriesTable`

`TimeSeriesTable` provides an interface to list rows within a time interval:

```go
    salesTable := keySpace.TimeSeriesTable("sale", "Created", "Id", Sale{}, 24 * time.Hour)
    //...
    results := []Sale{}
    err := salesTable.List(yesterdayTime, todayTime, &results).Run()
```

##### `MultiTimeSeriesTable`

`MultiTimeSeriesTable` is like a cross between `MultimapTable` and `TimeSeriesTable`. It can list rows within a time interval, and filtered by equality of a single field. The following lists sales in a time interval, by a certain seller:

```go
    salesTable := keySpace.MultiTimeSeriesTable("sale", "SellerId", "Created", "Id", Sale{}, 24 * time.Hour)
    //...
    results := []Sale{}
    err := salesTable.List("seller-1", yesterdayTime, todayTime, &results).Run()
```

##### Rough edges

###### Too long table names

In case you get the following error: 

```
Column family names shouldn't be more than 48 characters long (got "somelongishtablename_multitimeseries_start_id_24h0m0s")
```

You can use the TableName options to override the default internal ones:

```
tbl = tbl.WithOptions(Options{TableName: "somelongishtablename_mts_start_id_24h0m0s"})
```
