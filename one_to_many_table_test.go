package gocassa

import (
	"reflect"
	"testing"
)

type Customer2 struct {
	Id   string
	Name string
	Tag  string
}

func TestOneToManyTableInsertRead(t *testing.T) {
	tbl := ns.OneToManyTable("customer91", "Tag", "Id", Customer2{})
	createIf(tbl.(TableChanger), t)
	joe := Customer2{
		Id:   "33",
		Name: "Joe",
		Tag:  "A",
	}
	err := tbl.Set(joe)
	if err != nil {
		t.Fatal(err)
	}
	res := &Customer2{}
	err = tbl.Read("A", "33", res)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(*res, joe) {
		t.Fatal(*res, joe)
	}
	err = tbl.Read("B", "33", res)
	if err == nil {
		t.Fatal(*res)
	}
	err = tbl.Update("A", "33", map[string]interface{}{
		"Name": "John",
	})
	if err != nil {
		t.Fatal(err)
	}
	err = tbl.Read("A", "33", res)
	if err != nil {
		t.Fatal(err)
	}
	if res.Name != "John" {
		t.Fatal(*res)
	}
}

func TestOneToManyTableDelete(t *testing.T) {
	tbl := ns.OneToManyTable("customer92", "Tag", "Id", Customer2{})
	createIf(tbl.(TableChanger), t)
	joe := Customer2{
		Id:   "33",
		Name: "Joe",
		Tag:  "A",
	}
	err := tbl.Set(joe)
	if err != nil {
		t.Fatal(err)
	}
	res := &Customer2{}
	err = tbl.Read("A", "33", res)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(*res, joe) {
		t.Fatal(*res, joe)
	}
	err = tbl.Delete("A", "33")
	if err != nil {
		t.Fatal(err)
	}
	err = tbl.Read("A", "33", res)
	if err == nil {
		t.Fatal(res)
	}
}

func TestOneToManyTableMultiRead(t *testing.T) {
	tbl := ns.OneToManyTable("customer93", "Tag", "Id", Customer2{})
	createIf(tbl.(TableChanger), t)
	joe := Customer2{
		Id:   "33",
		Name: "Joe",
		Tag:  "A",
	}
	err := tbl.Set(joe)
	if err != nil {
		t.Fatal(err)
	}
	jane := Customer2{
		Id:   "34",
		Name: "Jane",
		Tag:  "A",
	}
	err = tbl.Set(jane)
	if err != nil {
		t.Fatal(err)
	}
	customers := &[]Customer2{}
	err = tbl.MultiRead("A", []interface{}{"33", "34"}, customers)
	if err != nil {
		t.Fatal(err)
	}
	if len(*customers) != 2 {
		t.Fatal("Expected to multiread 2 records, got %d", len(*customers))
	}
	if !reflect.DeepEqual((*customers)[0], joe) {
		t.Fatal("Expected to find joe, got %v", (*customers)[0])
	}
	if !reflect.DeepEqual((*customers)[1], jane) {
		t.Fatal("Expected to find jane, got %v", (*customers)[1])
	}
}
