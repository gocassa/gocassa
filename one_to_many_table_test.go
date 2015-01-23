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
	c, err := tbl.Read("A", "33")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(*(c.(*Customer2)), joe) {
		t.Fatal(*(c.(*Customer2)), joe)
	}
	c, err = tbl.Read("B", "33")
	if err == nil {
		t.Fatal(c)
	}
	err = tbl.Update("A", "33", map[string]interface{}{
		"Name": "John",
	})
	if err != nil {
		t.Fatal(err)
	}
	c, err = tbl.Read("A", "33")
	if err != nil {
		t.Fatal(err)
	}
	if c.(*Customer2).Name != "John" {
		t.Fatal(c)
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
	c, err := tbl.Read("A", "33")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(*(c.(*Customer2)), joe) {
		t.Fatal(*(c.(*Customer2)), joe)
	}
	err = tbl.Delete("A", "33")
	if err != nil {
		t.Fatal(err)
	}
	c, err = tbl.Read("A", "33")
	if err == nil || c != nil {
		t.Fatal(c, err)
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
	customers, err := tbl.MultiRead("A", "33", "34")
	if err != nil {
		t.Fatal(err)
	}
	if len(customers) != 2 {
		t.Fatal("Expected to multiread 2 records, got %d", len(customers))
	}
	if !reflect.DeepEqual(customers[0].(*Customer2), &joe) {
		t.Fatal("Expected to find joe, got %v", customers[0])
	}
	if !reflect.DeepEqual(customers[1].(*Customer2), &jane) {
		t.Fatal("Expected to find jane, got %v", customers[1])
	}
}
