gocassa
===

Gocassa is a high level library on top of [gocql](https://github.com/gocql/gocql).

Compared to gocql, it provides query building, data binding and it identifies certain use cases and provides different kind of tables for them. Unlike [cqlc](https://github.com/relops/cqlc), it does not require the user to generate code. It encourages the user to define their types in a way which is most natural for them.

#### Table types

##### Raw CQL Table

The Raw CQL table pretty much let's you write any CQL query, here is an example:

```
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
```

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
	sale, err := sales.Read("sale-1")
	if err != nil {
		panic(err)
	}
	fmt.Println(sale.(*Sale))
}


##### OneToOne Table

##### OneToMany Table

##### TimeSeries Table

#####
