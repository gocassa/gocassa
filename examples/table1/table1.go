package main

import (
	"fmt"
	"github.com/hailocab/gocassa"
	"time"
)

// This test assumes that cassandra is running on default port locally and
// that the keySpace called 'test' already exists.

type Sale struct {
	Id         string
	CustomerId string
	SellerId   string
	Price      int
	Created    time.Time
}

func main() {
	keySpace, err := gocassa.ConnectToKeySpace("test", []string{"127.0.0.1"}, "", "")
	if err != nil {
		panic(err)
	}
	salesTable := keySpace.Table("sale", &Sale{}, gocassa.Keys{
		PartitionKeys: []string{"Id"},
	})
	// Create the table - we ignore error intentionally
	salesTable.Create()

	// We insert the first record into our table - yay!
	err = salesTable.Set(Sale{
		Id:         "sale-1",
		CustomerId: "customer-1",
		SellerId:   "seller-1",
		Price:      42,
		Created:    time.Now(),
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
