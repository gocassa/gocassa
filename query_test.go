package cmagic

import (
	"fmt"
	"reflect"
	"testing"
)

type Customer struct {
	Id   string
	Name string
}

var ns KeySpace

func init() {
	var err error
	ns, err = New("test", "", "", []string{"127.0.0.1"})
	if err != nil {
		panic(err)
	}
	ns.DebugMode(true)
}

// cqlsh> CREATE KEYSPACE test WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
// cqlsh> CREATE TABLE test.customer (id text PRIMARY KEY, name text);
func TestEq(t *testing.T) {
	cs, err := ns.Table("customer", Customer{}, Keys{})
	if err != nil {
		t.Fatal(err)
	}
	err = cs.Set(Customer{
		Id:   "50",
		Name: "Joe",
	})
	if err != nil {
		t.Fatal(err)
	}
	res, err := cs.Where(Eq("Id", "50")).Query().Read()
	if err != nil {
		t.Fatal(err)
	}
	if len(res) == 0 {
		t.Fatal("Not found")
	}
	if res[0].(*Customer).Id != "50" {
		t.Fatal(res[0].(*Customer))
	}
}

func TestMultipleRowResults(t *testing.T) {
	name := "customer_multipletest"
	ns.(*K).Drop(name)
	cs, err := ns.Table(name, Customer{}, Keys{
		PartitionKeys:     []string{"Name"},
		ClusteringColumns: []string{"Id"},
	})
	if err != nil {
		t.Fatal(err)
	}
	err = cs.(*T).Create()
	if err != nil {
		t.Fatal(err)
	}
	cs.Set(Customer{
		Id:   "12",
		Name: "John",
	})
	cs.Set(Customer{
		Id:   "13",
		Name: "John",
	})
	res, err := cs.Where(Eq("Name", "John")).Query().Read()
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 2 {
		t.Fatal(len(res), res)
	}
}

func TestIn(t *testing.T) {
	cs, err := ns.Table("customer", Customer{}, Keys{})
	if err != nil {
		t.Fatal(err)
	}
	err = cs.Set(Customer{
		Id:   "100",
		Name: "Joe",
	})
	if err != nil {
		t.Fatal(err)
	}
	err = cs.Set(Customer{
		Id:   "200",
		Name: "Jane",
	})
	if err != nil {
		t.Fatal(err)
	}
	res, err := cs.Where(In("Id", "100", "200")).Query().Read()
	if len(res) != 2 {
		for _, v := range res {
			fmt.Println(*(v.(*Customer)))
		}
		t.Fatal("Not found", len(res))
	}
	if res[0].(*Customer).Id != "100" || res[1].(*Customer).Id != "200" {
		t.Fatal(res[0].(*Customer), res[1].(*Customer))
	}
}

// cqlsh> CREATE TABLE test.customer1 (id text, name text, PRIMARY KEY((id, name)));
func TestAnd(t *testing.T) {
	cs, err := ns.Table("customer1", Customer{}, Keys{})
	if err != nil {
		t.Fatal(err)
	}
	err = cs.Set(Customer{
		Id:   "100",
		Name: "Joe",
	})
	if err != nil {
		t.Fatal(err)
	}
	res, err := cs.Where(Eq("Id", "100"), Eq("Name", "Joe")).Query().Read()
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 1 {
		t.Fatal("Not found ", len(res))
	}
}

func TestQueryReturnError(t *testing.T) {
	cs, err := ns.Table("customer2", Customer{}, Keys{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = cs.Where(Eq("Id", "100"), Eq("Name", "Joe")).Query().Read()
	if err == nil {
		t.Fatal("Table customer2 does not exist - should return error")
	}
}

type Customer3 struct {
	Id     string
	Field1 string
	Field2 int
	Field3 int32
	Field4 int64
	Field5 float32
	Field6 float64
	Field7 bool
}

func TestTypesMarshal(t *testing.T) {
	c := Customer3{
		Id:     "1",
		Field1: "A",
		Field2: 1,
		Field3: 2,
		Field4: 3,
		Field5: 4.0,
		Field6: 5.0,
		Field7: true,
	}
	tbl, err := ns.Table("customer3", Customer3{}, Keys{PartitionKeys: []string{"Id"}})
	if err != nil {
		t.Fatal(err)
	}
	err = tbl.(*T).Create()
	if ex, err := ns.(*K).Exists("customer3"); err != nil {
		t.Fatal(err)
	} else if !ex {
		err := tbl.(*T).Create()
		if err != nil {
			t.Fatal("Create table failed :", err)
		}
	}
	if err = tbl.Set(c); err != nil {
		t.Fatal(err)
	}
	res, err := tbl.Where(Eq("Id", "1")).Query().Read()
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 1 {
		t.Fatal("No results returned")
	}
	if !reflect.DeepEqual(c, *res[0].(*Customer3)) {
		t.Fatal(c, *res[0].(*Customer3))
	}
}
