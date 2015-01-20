package cmagic

import (
	"reflect"
	"testing"
)

func TestOnyToOneTTable(t *testing.T) {
	tbl := ns.OnyToOneTTable("customer81", "Id", Customer{})
	createIf(ns, tbl.(*OnyToOneT).t, t)
	joe := Customer{
		Id: "33",
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

func TestOnyToOneTUpdate(t *testing.T) {
	tbl := ns.OnyToOneTTable("customer82", "Id", Customer{})
	createIf(ns, tbl.(*OnyToOneT).t, t)
	joe := Customer{
		Id: "33",
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
