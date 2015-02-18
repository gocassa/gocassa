package gocassa

import (
	"reflect"
	"testing"
)

func TestMapTable(t *testing.T) {
	tbl := ns.MapTable("customer81", "Id", Customer{})
	createIf(tbl.(TableChanger), t)
	joe := Customer{
		Id:   "33",
		Name: "Joe",
	}
	err := tbl.Set(joe).Run()
	if err != nil {
		t.Fatal(err)
	}
	res := &Customer{}
	err = tbl.Read("33", res).Run()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(*res, joe) {
		t.Fatal(*res, joe)
	}
	err = tbl.Delete("33").Run()
	if err != nil {
		t.Fatal(err)
	}
	err = tbl.Read("33", res).Run()
	if err == nil {
		t.Fatal(res)
	}
}

func TestMapTableUpdate(t *testing.T) {
	tbl := ns.MapTable("customer82", "Id", Customer{})
	createIf(tbl.(TableChanger), t)
	joe := Customer{
		Id:   "33",
		Name: "Joe",
	}
	err := tbl.Set(joe).Run()
	if err != nil {
		t.Fatal(err)
	}
	res := &Customer{}
	err = tbl.Read("33", res).Run()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(*res, joe) {
		t.Fatal(*res, joe)
	}
	err = tbl.Update("33", map[string]interface{}{
		"Name": "John",
	}).Run()
	if err != nil {
		t.Fatal(err)
	}
	err = tbl.Read("33", res).Run()
	if err != nil {
		t.Fatal(res, err)
	}
	if res.Name != "John" {
		t.Fatal(res)
	}
}

func TestMapTableMultiRead(t *testing.T) {
	tbl := ns.MapTable("customer83", "Id", Customer{})
	createIf(tbl.(TableChanger), t)
	joe := Customer{
		Id:   "33",
		Name: "Joe",
	}
	err := tbl.Set(joe).Run()
	if err != nil {
		t.Fatal(err)
	}
	jane := Customer{
		Id:   "34",
		Name: "Jane",
	}
	err = tbl.Set(jane).Run()
	if err != nil {
		t.Fatal(err)
	}
	customers := &[]Customer{}
	err = tbl.MultiRead([]interface{}{"33", "34"}, customers).Run()
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
