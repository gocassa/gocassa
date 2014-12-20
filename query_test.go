package cmagic

import (
	"testing"
)

type Customer struct {
	Id string
	Name string
}

// cqlsh> CREATE KEYSPACE test WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
// cqlsh> CREATE TABLE test.customer (id text PRIMARY KEY, name text);
func TestReadById(t *testing.T) {
	ns, err := New("test", "", "", []string{"127.0.0.1"})
	if err != nil {
		t.Fatal(ns, err)
	}
	cs := ns.Table("customer", Customer{}, Keys{})
	err = cs.Insert(Customer{
		Id: "50",
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