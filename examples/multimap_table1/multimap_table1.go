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
	// "SellerId" is the field we will use to query sales:
	// MultimapTable enables us to return all the sales where SellerId equals
	// to a certain value.
	salesTable := keySpace.MultimapTable("sale", "SellerId", "Id", &Sale{})
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
	// One thing we have to notice here is that to read a single row,
	// we have to know the SellerId and SaleId as well. This is due to how
	// data is structured in cassandra .
	if err := salesTable.Read("seller-1", "sale-1", &result).Run(); err != nil {
		panic(err)
	}
	fmt.Println(result)

	// As an upside, now we can actually list rows based on a criteria. Let's insert an other row
	// before Listing - it is no fun to have a list with one element only ;)
	err = salesTable.Set(Sale{
		Id:         "sale-2",
		CustomerId: "customer-1",
		SellerId:   "seller-1",
		Price:      55,
		Created:    time.Now(),
	}).Run()
	if err != nil {
		panic(err)
	}

	resultList := []Sale{}
	if err := salesTable.List("seller-1", nil, 0, &resultList).Run(); err != nil {
		panic(err)
	}
	fmt.Println("Our result list now has 2 rows. Amazing!!!", resultList)

	// To Update we also need to know both SellerId and SaleId:
	fmt.Printf("Updating sales with SellerId %v and Id %v \"sale-1\" with Price = 110\n", "seller-1", "sale-1")
	err = salesTable.Update("seller-1", "sale-1", map[string]interface{}{
		"Price": 110,
	}).Run()
	if err != nil {
		panic(err)
	}

	fmt.Println("Reading sales record again: ")
	// Read the row again:
	if err := salesTable.Read("seller-1", "sale-1", &result).Run(); err != nil {
		panic(err)
	}
	fmt.Println(result)

	// You might also wonder how paging works.
	// With cassandra, the idiomatic way is not page number based paging, but
	// rather, you continue the next page where the last one stopped.
	// To achieve this, you usually query 1 more result than you need, let's say 21,
	// and use the last element as a "cursor"

	// Insert some data to paginate:
	for i := 0; i < 50; i++ {
		err = salesTable.Set(Sale{
			Id:         fmt.Sprintf("sale-%v", i),
			CustomerId: "customer-1",
			SellerId:   "seller-1",
			Price:      55,
			Created:    time.Now(),
		}).Run()
		if err != nil {
			panic(err)
		}
	}

	fmt.Println("First 10 sales of seller-1:")
	err = salesTable.List("seller-1", nil, 11, &resultList).Run()
	if err != nil {
		panic(err)
	}
	fmt.Println(resultList)
	if len(resultList) > 10 {
		err = salesTable.List("seller-1", resultList[10].Id, 11, &resultList).Run()
		if err != nil {
			panic(err)
		}
		fmt.Println("Second 10 sales of seller-1:")
		fmt.Println(resultList)
	}
}
