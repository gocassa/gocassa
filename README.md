gocassa
=======

[![GoDoc](https://img.shields.io/badge/godoc-reference-blue.svg "GoDoc")](http://godoc.org/github.com/hailocab/gocassa) 
[![Build Status](https://img.shields.io/travis/hailocab/gocassa/master.svg "Build Status")](https://travis-ci.org/hailocab/gocassa) 

Gocassa is a high-level library on top of [gocql](https://github.com/gocql/gocql).

Current version: v1.3.0

Compared to gocql it provides query building, adds data binding, and provides easy-to-use "recipe" tables for common query use-cases. Unlike [cqlc](https://github.com/relops/cqlc), it does not use code generation.

For docs, see: [https://godoc.org/github.com/hailocab/gocassa](https://godoc.org/github.com/hailocab/gocassa)

## Usage

Below is a basic example showing how to connect to a Cassandra cluster and setup a simple table. For more advanced examples see the "Table Types" section below.

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
    salesTable := keySpace.Table("sale", &Sale{}, gocassa.Keys{
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

You can pass additional options to a gocassa `Op` to further configure your queries, for example the following query orders the results by the field "Name" in descending order and limits the results to a total of 100.

```go
err := salesTable.List("seller-1", nil, 0, &results).WithOptions(gocassa.Options{
    ClusteringOrder: []ClusteringOrderColumn{
        {DESC, "Name"},
    },
    Limit: 100,
}).Run()
```

### Table Types

Gocassa provides multiple table types with their own unique interfaces:
- a raw CQL table called simply `Table` - this lets you do pretty much any query imaginable
- and a number of single purpose 'recipe' tables (`Map`, `Multimap`, `TimeSeries`, `MultiTimeSeries`, `MultiMapMultiKey`), which aims to help the user by having a simplified interface tailored to a given common query use case

#### Table

```go
    salesTable := keySpace.Table("sale", &Sale{}, gocassa.Keys{
        PartitionKeys: []string{"Id"},
    })
    result := Sale{}
    err := salesTable.Where(gocassa.Eq("Id", "sale-1")).ReadOne(&result).Run()
```
[link to this example](https://github.com/hailocab/gocassa/blob/master/examples/table1/table1.go)

#### MapTable

`MapTable` provides only very simple [CRUD](http://en.wikipedia.org/wiki/Create,_read,_update_and_delete) functionality:

```go
    // …
    salesTable := keySpace.MapTable("sale", "Id", &Sale{})
    result := Sale{}
    salesTable.Read("sale-1", &result).Run()
}
```
[link to this example](https://github.com/hailocab/gocassa/blob/master/examples/map_table1/map_table1.go)

Read, Set, Update, and Delete all happen by "Id".

#### MultimapTable

`MultimapTable` can list rows filtered by equality of a single field (eg. list sales based on their `sellerId`):

```go
    salesTable := keySpace.MultimapTable("sale", "SellerId", "Id", &Sale{})
    // …
    results := []Sale{}
    err := salesTable.List("seller-1", nil, 0, &results).Run()
```
[link to this example](https://github.com/hailocab/gocassa/blob/master/examples/multimap_table1/multimap_table1.go)

For examples on how to do pagination or Update with this table, refer to the example (linked under code snippet). 

#### TimeSeriesTable

`TimeSeriesTable` provides an interface to list rows within a time interval:

```go
    salesTable := keySpace.TimeSeriesTable("sale", "Created", "Id", &Sale{}, 24 * time.Hour)
    //...
    results := []Sale{}
    err := salesTable.List(yesterdayTime, todayTime, &results).Run()
```

#### MultiTimeSeriesTable

`MultiTimeSeriesTable` is like a cross between `MultimapTable` and `TimeSeriesTable`. It can list rows within a time interval, and filtered by equality of a single field. The following lists sales in a time interval, by a certain seller:

```go
    salesTable := keySpace.MultiTimeSeriesTable("sale", "SellerId", "Created", "Id", &Sale{}, 24 * time.Hour)
    //...
    results := []Sale{}
    err := salesTable.List("seller-1", yesterdayTime, todayTime, &results).Run()
```

#### MultiMapMultiKeyTable

`MultiMapMultiKeyTable` can perform CRUD operations on rows filtered by equality of multiple fields (eg. read a sale based on their `city` , `sellerId` and `Id` of the sale):

```go
    salePartitionKeys := []Sale{"City"}
    saleClusteringKeys := []Sale{"SellerId","Id"}
    salesTable := keySpace.MultimapMultiKeyTable("sale", salePartitionKeys, saleClusteringKeys, Sale{})
    // …
    result := Sale{}
    saleFieldCity = salePartitionKeys[0]
    saleFieldSellerId = saleClusteringKeys[0]
    saleFieldSaleId = saleClusteringKeys[1]

    field := make(map[string]interface{})
    id := make(map[string]interface{})


    field[saleFieldCity] = "London"
    id[saleFieldSellerId] = "141-dasf1-124"
    id[saleFieldSaleId] = "512hha232"

    err := salesTable.Read(field, id , &result).Run()
```

## Encoding/Decoding data structures

When setting `structs` in gocassa the library first converts your value to a map. Each exported field is added to the map unless

- the field's tag is "-", or
- the field is empty and its tag specifies the "omitempty" option

Each fields default name in the map is the field name but can be specified in the struct field's tag value. The "cql" key in the struct field's tag value is the key name, followed by an optional comma and options. Examples:

```go
// Field is ignored by this package.
Field int `cql:"-"`
// Field appears as key "myName".
Field int `cql:"myName"`
// Field appears as key "myName" and
// the field is omitted from the object if its value is empty,
// as defined above.
Field int `cql:"myName,omitempty"`
// Field appears as key "Field" (the default), but
// the field is skipped if empty.
// Note the leading comma.
Field int `cql:",omitempty"`
// All fields in the EmbeddedType are squashed into the parent type.
EmbeddedType `cql:",squash"`
```

When encoding maps with non-string keys the key values are automatically converted to strings where possible, however it is recommended that you use strings where possible (for example map[string]T).

## Troubleshooting

### Too long table names

In case you get the following error: 

```
Column family names shouldn't be more than 48 characters long (got "somelongishtablename_multitimeseries_start_id_24h0m0s")
```

You can use the TableName options to override the default internal ones:

```go
tbl = tbl.WithOptions(Options{TableName: "somelongishtablename_mts_start_id_24h0m0s"})
```
