package cmagic

import (
	"reflect"
	"testing"
)

func TestOneToOneTable(t *testing.T) {
	tbl := ns.OneToOneTable("customer91", "Id", Customer{})
	createIf(ns, tbl.(*oneToOne).t, t)
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
}