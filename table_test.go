package cmagic

import(
	"fmt"
	"math/rand"
	"testing"
	"time"
)

// cqlsh> CREATE TABLE test.customer1 (id text, name text, PRIMARY KEY((id, name)));
func TestTables(t *testing.T) {
	res, err := ns.(*keySpace).Tables()
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
	if ex, err := ns.(*keySpace).Exists(name); ex && err == nil {
		err = ns.(*keySpace).Drop(name)
		if err != nil {
			t.Fatal(err)
		}
	} else if err != nil {
		t.Fatal(err)
	}
	cs := ns.Table(name, Customer{}, Keys{
		PartitionKeys: []string{"Id", "Name"},
	})
	err := cs.(*table).Create()
	if err != nil {
		t.Fatal(err)
	}
	err = cs.Set(Customer{
		Id: "1001",
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
	err = ns.(*keySpace).Drop(name)
	if err != nil {
		t.Fatal(err)
	}
}