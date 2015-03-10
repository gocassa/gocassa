package main

import (
	"fmt"
	"github.com/hailocab/gocassa"
	"time"
)

// This test assumes that cassandra is running on default port locally and
// that the keySpace called 'test' already exists.

func test() {
	keySpace, err := gocassa.ConnectToKeySpace("test", []string{"127.0.0.1"}, "", "")
	if err != nil {
		panic(err)
	}
	salesTable := keySpace.MapTable("sale", "Id", Sale{})
	// Create the table - we ignore error intentionally
	salesTable.Create()

	// We insert the first record into our table - yay!
	err = salesTable.Set(Sale{
		Id:         "sale-1",
		CustomerId: "customer-1",
		SellerId:   "seller-1",
		Price:      42,
		Created:    time.Now(),
	})
	if err != nil {
		panic(err)
	}

	result := Sale{}
	if err := salesTable.Read("sale-1", &result); err != nil {
		panic(err)
	}
	fmt.Println(result)
}
