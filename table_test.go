package cmagic

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func createIf(cs TableChanger, tes *testing.T) {
	err := cs.(TableChanger).Recreate()
	if err != nil {
		tes.Fatal(err)
	}
}

// cqlsh> CREATE TABLE test.customer1 (id text, name text, PRIMARY KEY((id, name)));
func TestTables(t *testing.T) {
	res, err := ns.(*k).Tables()
	if err != nil {
		t.Fatal(err)
	}
	if len(res) == 0 {
		t.Fatal("Not found ", len(res))
	}
}

func TestCreateTable(t *testing.T) {
	rand.Seed(time.Now().Unix())
	name := fmt.Sprintf("customer_%v", rand.Int()%100)
	cs := ns.Table(name, Customer{}, Keys{
		PartitionKeys: []string{"Id", "Name"},
	})
	createIf(cs, t)
	err := cs.Set(Customer{
		Id:   "1001",
		Name: "Joe",
	})
	if err != nil {
		t.Fatal(err)
	}
	res, err := cs.Where(Eq("Id", "1001"), Eq("Name", "Joe")).Query().Read()
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 1 {
		t.Fatal("Not found ", len(res))
	}
	err = ns.(*k).DropTable(name)
	if err != nil {
		t.Fatal(err)
	}
}

type Customer1 struct {
	Id       string
	MaxSpeed int
	Brand    string
}

func TestCreateTable2(t *testing.T) {

}
