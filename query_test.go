package cmagic

import (
	"testing"
)

type Customer struct {
	Id string
	Name string
}

var ns KeySpace

func init() {
	var err error
	ns, err = New("test", "", "", []string{"127.0.0.1"})
	if err != nil {
		panic(err)
	}
}

// cqlsh> CREATE KEYSPACE test WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
// cqlsh> CREATE TABLE test.customer (id text PRIMARY KEY, name text);
func TestEq(t *testing.T) {
	cs := ns.Table("customer", Customer{}, Keys{})
	err := cs.Set(Customer{
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

func TestIn(t *testing.T) {
	cs := ns.Table("customer", Customer{}, Keys{})
	err := cs.Set(Customer{
		Id: "100",
		Name: "Joe",
	})
	if err != nil {
		t.Fatal(err)
	}
	err = cs.Set(Customer{
		Id: "200",
		Name: "Jane",
	})
	if err != nil {
		t.Fatal(err)
	}
	res, err := cs.Where(In("Id", "100", "200")).Query().Read()
	if len(res) != 2 {
		t.Fatal("Not found", len(res))
	}
	if res[0].(*Customer).Id != "100" || res[1].(*Customer).Id != "200" {
		t.Fatal(res[0].(*Customer), res[1].(*Customer))
	}
}

// cqlsh> CREATE TABLE test.customer1 (id text, name text, PRIMARY KEY((id, name)));
func TestAnd(t *testing.T) {
	cs := ns.Table("customer1", Customer{}, Keys{})
	err := cs.Set(Customer{
		Id: "100",
		Name: "Joe",
	})
	if err != nil {
		t.Fatal(err)
	}
	res, err := cs.Where(Eq("Id", "100"), Eq("Name", "Joe")).Query().Read()
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 1 {
		t.Fatal("Not found ", len(res))
	}
}

func TestQueryReturnError(t *testing.T) {
	cs := ns.Table("customer2", Customer{}, Keys{})
	_, err := cs.Where(Eq("Id", "100"), Eq("Name", "Joe")).Query().Read()
	if err == nil {
		t.Fatal("Table customer2 does not exist - should return error")
	}
}