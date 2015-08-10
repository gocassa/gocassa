package gocassa

import (
	"fmt"
	"math/rand"
	"strings"
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
	}).Run()
	if err != nil {
		t.Fatal(err)
	}
	res := &[]Customer{}
	err = cs.Where(Eq("Id", "1001"), Eq("Name", "Joe")).Read(res).Run()
	if err != nil {
		t.Fatal(err)
	}
	if len(*res) != 1 {
		t.Fatal("Not found ", len(*res))
	}
	err = ns.(*k).DropTable(name)
	if err != nil {
		t.Fatal(err)
	}
}

func TestCreateStatement(t *testing.T) {
	cs := ns.Table("something", Customer{}, Keys{
		PartitionKeys: []string{"Id", "Name"},
	})
	str, err := cs.CreateStatement()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(str, "something") {
		t.Fatal(str)
	}
	str, err = cs.WithOptions(Options{TableName: "funky"}).CreateStatement()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(str, "funky") {
		t.Fatal(str)
	}
	// if clustering order is not specified, it should omit the clustering order by and use the default
	if strings.Contains(str, "WITH CLUSTERING ORDER BY") {
		t.Fatal(str)
	}
}

func TestCreateStatementWithClusteringOrder(t *testing.T) {
	order := &ClusteringOrder{
		Columns: []ClusteringOrderColumn{
			ClusteringOrderColumn{
				Ascending: false,
				Column:    "Name",
			},
		},
	}
	cs := ns.Table("something", Customer{}, Keys{
		PartitionKeys:     []string{"Id"},
		ClusteringColumns: []string{"Name"},
	})
	opts := Options{Order: order}
	str, err := cs.WithOptions(opts).CreateStatement()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(str, "something") {
		t.Fatal(str)
	}
	if !strings.Contains(str, "WITH CLUSTERING ORDER BY (Name DESC)") {
		t.Fatal(str)
	}
}

func TestCreateStatementWithMultipleClusteringOrder(t *testing.T) {
	order := &ClusteringOrder{
		Columns: []ClusteringOrderColumn{
			ClusteringOrderColumn{
				Ascending: false,
				Column:    "Name",
			},
			ClusteringOrderColumn{
				Ascending: true,
				Column:    "Tag",
			},
		},
	}
	cs := ns.Table("something", Customer2{}, Keys{
		PartitionKeys:     []string{"Id"},
		ClusteringColumns: []string{"Name", "Tag"},
	})
	opts := Options{Order: order}
	str, err := cs.WithOptions(opts).CreateStatement()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(str, "something") {
		t.Fatal(str)
	}
	if !strings.Contains(str, "WITH CLUSTERING ORDER BY (Name DESC, Tag ASC)") {
		t.Fatal(str)
	}
}
