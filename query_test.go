package gocassa

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
	kname := "test_ihopeudonthaveakeyspacenamedlikedthis"
	var err error
	c, err := Connect([]string{"127.0.0.1"}, "", "")
	if err != nil {
		panic(err)
	}
	err = c.DropKeySpace(kname)
	if err != nil {
		panic(err)
	}
	err = c.CreateKeySpace(kname)
	if err != nil {
		panic(err)
	}
	ns, err = ConnectToKeySpace(kname, []string{"127.0.0.1"}, "", "")
	if err != nil {
		panic(err)
	}
	//ns.DebugMode(true)
}

func TestEq(t *testing.T) {
	cs := ns.Table("customer", Customer{}, Keys{PartitionKeys: []string{"Id"}})
	createIf(cs.(TableChanger), t)
	err := cs.Set(Customer{
		Id:   "50",
		Name: "Joe",
	})
	if err != nil {
		t.Fatal(err)
	}
	res := &[]Customer{}
	err = cs.Where(Eq("Id", "50")).Query().Read(res)
	if err != nil {
		t.Fatal(err)
	}
	if len(*res) == 0 {
		t.Fatal("Not found")
	}
	if (*res)[0].Id != "50" {
		t.Fatal((*res)[0])
	}
}

func TestMultipleRowResults(t *testing.T) {
	name := "customer_multipletest"
	cs := ns.Table(name, Customer{}, Keys{
		PartitionKeys:     []string{"Name"},
		ClusteringColumns: []string{"Id"},
	})
	err := cs.(TableChanger).Recreate()
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
	res := &[]Customer{}
	err = cs.Where(Eq("Name", "John")).Query().Read(res)
	if err != nil {
		t.Fatal(err)
	}
	if len(*res) != 2 {
		t.Fatal(res)
	}
}

func TestIn(t *testing.T) {
	cs := ns.Table("customer", Customer{}, Keys{
		PartitionKeys: []string{"Id"},
	})
	err := cs.Set(Customer{
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
	res := &[]Customer{}
	err = cs.Where(In("Id", "100", "200")).Query().Read(res)
	if len(*res) != 2 {
		for _, v := range *res {
			fmt.Println(v)
		}
		t.Fatal("Not found", res)
	}
	if (*res)[0].Id != "100" || (*res)[1].Id != "200" {
		t.Fatal((*res)[0], (*res)[1])
	}
}

func TestAnd(t *testing.T) {
	cs := ns.Table("customer1", Customer{}, Keys{PartitionKeys: []string{"Id", "Name"}})
	createIf(cs.(TableChanger), t)
	err := cs.Set(Customer{
		Id:   "100",
		Name: "Joe",
	})
	if err != nil {
		t.Fatal(err)
	}
	res := &[]Customer{}
	err = cs.Where(Eq("Id", "100"), Eq("Name", "Joe")).Query().Read(res)
	if err != nil {
		t.Fatal(err)
	}
	if len(*res) != 1 {
		t.Fatal("Not found ", res)
	}
}

func TestQueryReturnError(t *testing.T) {
	cs := ns.Table("customer2", Customer{}, Keys{})
	res := &[]Customer{}
	err := cs.Where(Eq("Id", "100"), Eq("Name", "Joe")).Query().Read(res)
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
	Field8 []byte
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
		Field8: []byte{'a', 'b', 'c'},
	}
	tbl := ns.Table("customer3", Customer3{}, Keys{PartitionKeys: []string{"Id"}})
	createIf(tbl.(TableChanger), t)
	if err := tbl.Set(c); err != nil {
		t.Fatal(err)
	}
	res := &[]Customer3{}
	err := tbl.Where(Eq("Id", "1")).Query().Read(res)
	if err != nil {
		t.Fatal(err)
	}
	if len(*res) != 1 {
		t.Fatal(res)
	}
	if !reflect.DeepEqual(c, (*res)[0]) {
		t.Fatal(c, (*res)[0])
	}
}
