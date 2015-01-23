package gocassa

import (
	"reflect"
	"testing"
)

func TestOneToOneTable(t *testing.T) {
	tbl := ns.OneToOneTable("customer81", "Id", Customer{})
	createIf(tbl.(TableChanger), t)
	joe := Customer{
		Id:   "33",
		Name: "Joe",
	}
	err := tbl.Set(joe)
	if err != nil {
		t.Fatal(err)
	}
	c, err := tbl.Read("33")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(*(c.(*Customer)), joe) {
		t.Fatal(*(c.(*Customer)), joe)
	}
	err = tbl.Delete("33")
	if err != nil {
		t.Fatal(err)
	}
	c, err = tbl.Read("33")
	if c != nil || err == nil {
		t.Fatal(c, err)
	}
}

func TestOneToOneTableUpdate(t *testing.T) {
	tbl := ns.OneToOneTable("customer82", "Id", Customer{})
	createIf(tbl.(TableChanger), t)
	joe := Customer{
		Id:   "33",
		Name: "Joe",
	}
	err := tbl.Set(joe)
	if err != nil {
		t.Fatal(err)
	}
	c, err := tbl.Read("33")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(*(c.(*Customer)), joe) {
		t.Fatal(*(c.(*Customer)), joe)
	}
	err = tbl.Update("33", map[string]interface{}{
		"Name": "John",
	})
	if err != nil {
		t.Fatal(err)
	}
	c, err = tbl.Read("33")
	if err != nil {
		t.Fatal(c, err)
	}
	if c.(*Customer).Name != "John" {
		t.Fatal(c)
	}
}

func TestOneToOneTableMultiRead(t *testing.T) {
	tbl := ns.OneToOneTable("customer83", "Id", Customer{})
	createIf(tbl.(TableChanger), t)
	joe := Customer{
		Id:   "33",
		Name: "Joe",
	}
	err := tbl.Set(joe)
	if err != nil {
		t.Fatal(err)
	}
	jane := Customer{
		Id:   "34",
		Name: "Jane",
	}
	err = tbl.Set(jane)
	if err != nil {
		t.Fatal(err)
	}
	customers, err := tbl.MultiRead("33", "34")
	if err != nil {
		t.Fatal(err)
	}
	if len(customers) != 2 {
		t.Fatal("Expected to multiread 2 records, got %d", len(customers))
	}
	if !reflect.DeepEqual(customers[0].(*Customer), &joe) {
		t.Fatal("Expected to find joe, got %v", customers[0])
	}
	if !reflect.DeepEqual(customers[1].(*Customer), &jane) {
		t.Fatal("Expected to find jane, got %v", customers[1])
	}
}
