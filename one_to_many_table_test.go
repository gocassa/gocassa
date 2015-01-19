package cmagic

import (
	"reflect"
	"testing"
)

type Customer2 struct {
	Id string
	Name string
	Tag string
}

func TestOneToManyTableInsertRead(t *testing.T) {
	tbl := ns.OneToManyTable("customer91", "Tag", "Id", Customer2{})
	createIf(ns, tbl.(*oneToMany).t, t)
	joe := Customer2{
		Id: "33",
		Name: "Joe",
		Tag: "A",
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
	createIf(ns, tbl.(*oneToMany).t, t)
	joe := Customer2{
		Id: "33",
		Name: "Joe",
		Tag: "A",
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