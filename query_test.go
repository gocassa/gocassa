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
	}).Run()
	if err != nil {
		t.Fatal(err)
	}
	res := &[]Customer{}
	err = cs.Where(Eq("Id", "50")).Query().Read(res).Run()
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
	err = cs.Set(Customer{
		Id:   "12",
		Name: "John",
	}).Add(cs.Set(Customer{
		Id:   "13",
		Name: "John",
	})).Run()
	if err != nil {
		t.Fatal(err)
	}

	res := &[]Customer{}
	err = cs.Where(Eq("Name", "John")).Query().Read(res).Run()
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
	}).Add(cs.Set(Customer{
		Id:   "200",
		Name: "Jane",
	})).Run()
	if err != nil {
		t.Fatal(err)
	}
	res := &[]Customer{}
	err = cs.Where(In("Id", "100", "200")).Query().Read(res).Run()
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
	}).Run()
	if err != nil {
		t.Fatal(err)
	}
	res := &[]Customer{}
	err = cs.Where(Eq("Id", "100"), Eq("Name", "Joe")).Query().Read(res).Run()
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
	err := cs.Where(Eq("Id", "100"), Eq("Name", "Joe")).Query().Read(res).Run()
	if err == nil {
		t.Fatal("Table customer2 does not exist - should return error")
	}
}

func TestRowNotFoundError(t *testing.T) {
	cs := ns.MapTable("customer", "Id", Customer{})
	createIf(cs.(TableChanger), t)
	c := &Customer{}
	err := cs.Read("8sad8as8ds8u34", c).Run()
	_, ok := err.(RowNotFoundError)
	if !ok {
		t.Fatal(err)
	}
}

type Customer3 struct {
	Id      string
	Field1  string
	Field2  int
	Field3  int32
	Field4  int64
	Field5  float32
	Field6  float64
	Field7  bool
	Field8  []byte
	Field9  []string
	Field10 []int
	Field11 []int32
	Field12 []float32
	Field13 []float64
	Field14 []bool
}

func TestTypesMarshal(t *testing.T) {
	c := Customer3{
		Id:      "1",
		Field1:  "A",
		Field2:  1,
		Field3:  2,
		Field4:  3,
		Field5:  4.0,
		Field6:  5.0,
		Field7:  true,
		Field8:  []byte{'a', 'b', 'c'},
		Field9:  []string{"a", "b", "c"},
		Field10: []int{1, 2, 3},
		Field11: []int32{1, 2, 3},
		Field12: []float32{1.0, 2.0, 3.33},
		Field13: []float64{1.0, 2.0, 3.33},
		Field14: []bool{false, true},
	}
	tbl := ns.Table("customer3", Customer3{}, Keys{PartitionKeys: []string{"Id"}})
	createIf(tbl.(TableChanger), t)
	if err := tbl.Set(c).Run(); err != nil {
		t.Fatal(err)
	}
	res := &[]Customer3{}
	err := tbl.Where(Eq("Id", "1")).Query().Read(res).Run()
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

type Customer4 struct {
	Id         string
	FavNums    []int
	FavStrings []string
}

func TestUpdateList(t *testing.T) {
	tbl := ns.Table("customer4", Customer4{}, Keys{PartitionKeys: []string{"Id"}})
	createIf(tbl.(TableChanger), t)
	c := Customer4{
		Id:         "99",
		FavNums:    []int{1, 2, 3},
		FavStrings: []string{"yo", "boop", "woot"},
	}
	if err := tbl.Set(c).Run(); err != nil {
		t.Fatal(err)
	}
	f := tbl.Where(Eq("Id", "99"))
	err := f.Update(map[string]interface{}{
		"FavNums": ListRemove(2),
	}).Add(f.Update(map[string]interface{}{
		"FavStrings": ListRemove("2"),
	})).Run()
	if err != nil {
		t.Fatal(err)
	}
	err = f.Query().ReadOne(&c).Run()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(c.FavStrings, []string{"yo", "boop", "woot"}) || !reflect.DeepEqual(c.FavNums, []int{1, 3}) {
		t.Fatal(c)
	}
	err = f.Update(map[string]interface{}{
		"FavStrings": ListSetAtIndex(1, "changedBoop"),
	}).Run()
	if err != nil {
		t.Fatal(err)
	}
	err = f.Query().ReadOne(&c).Run()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(c.FavStrings, []string{"yo", "changedBoop", "woot"}) {
		t.Fatal(c)
	}
	err = f.Update(map[string]interface{}{
		"FavStrings": ListAppend("last"),
	}).Run()
	if err != nil {
		t.Fatal(err)
	}
	err = f.Query().ReadOne(&c).Run()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(c.FavStrings, []string{"yo", "changedBoop", "woot", "last"}) {
		t.Fatal(c)
	}
	err = f.Update(map[string]interface{}{
		"FavStrings": ListPrepend("first"),
	}).Run()
	if err != nil {
		t.Fatal(err)
	}
	err = f.Query().ReadOne(&c).Run()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(c.FavStrings, []string{"first", "yo", "changedBoop", "woot", "last"}) {
		t.Fatal(c)
	}
}

func TestCQLInjection(t *testing.T) {
	tbl := ns.Table("customer45", Customer4{}, Keys{PartitionKeys: []string{"Id"}})
	createIf(tbl.(TableChanger), t)
	c := Customer4{
		Id:         "99",
		FavStrings: []string{"yo", "boop", "woot"},
	}
	if err := tbl.Set(c).Run(); err != nil {
		t.Fatal(err)
	}
	// At the moment we don't have batch so we just try to mess up the CQL query with a single quote - if
	// it can be messed up then we are vulnerable
	err := tbl.Where(Eq("Id", "99")).Update(map[string]interface{}{
		"FavStrings": ListRemove("'"),
	}).Run()
	if err != nil {
		t.Fatal(err)
	}
}
