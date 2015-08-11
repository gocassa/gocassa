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

func TestClusteringOrder(t *testing.T) {
	order := &ClusteringOrder{
		Columns: []ClusteringOrderColumn{
			ClusteringOrderColumn{
				Ascending: false,
				Column:    "Id",
			},
		},
	}
	options := Options{Order: order}
	name := "customer_by_name"
	cs := ns.Table(name, Customer{}, Keys{
		PartitionKeys:     []string{"Name"},
		ClusteringColumns: []string{"Id"},
	}).WithOptions(options)
	createIf(cs, t)

	customers := []Customer{
		Customer{
			Id:   "1001",
			Name: "Brian",
		},
		Customer{
			Id:   "1002",
			Name: "Adam",
		},
		Customer{
			Id:   "1003",
			Name: "Brian",
		},
		Customer{
			Id:   "1004",
			Name: "Brian",
		},
	}

	for _, c := range customers {
		err := cs.Set(c).Run()
		if err != nil {
			t.Fatal(err)
		}
	}
	res := &[]Customer{}
	err := cs.Where(Eq("Name", "Brian")).Read(res).Run()
	if err != nil {
		t.Fatal(err)
	}

	expected := []string{"1004", "1003", "1001"}
	if len(*res) != len(expected) {
		t.Fatal("Expected", len(*res), " results, got", len(*res))
	}
	for i, id := range expected {
		if (*res)[i].Id != id {
			t.Fatal("Got result out of order. i:", i, "expected ID:", id, "actual ID:", (*res)[i].Id)
		}
	}
}

func TestClusteringOrderMultipl(t *testing.T) {
	order := &ClusteringOrder{
		Columns: []ClusteringOrderColumn{
			ClusteringOrderColumn{
				Ascending: false,
				Column:    "Tag",
			},
			ClusteringOrderColumn{
				Ascending: false,
				Column:    "Id",
			},
		},
	}
	options := Options{Order: order}
	name := "customer_by_name2"
	cs := ns.Table(name, Customer2{}, Keys{
		PartitionKeys:     []string{"Name"},
		ClusteringColumns: []string{"Tag", "Id"},
	}).WithOptions(options)
	createIf(cs, t)

	customers := []Customer2{
		Customer2{
			Id:   "1001",
			Name: "Brian",
			Tag:  "B",
		},
		Customer2{
			Id:   "1002",
			Name: "Adam",
			Tag:  "A",
		},
		Customer2{
			Id:   "1003",
			Name: "Brian",
			Tag:  "A",
		},
		Customer2{
			Id:   "1004",
			Name: "Brian",
			Tag:  "B",
		},
	}

	for _, c := range customers {
		err := cs.Set(c).Run()
		if err != nil {
			t.Fatal(err)
		}
	}
	res := &[]Customer2{}
	err := cs.Where(Eq("Name", "Brian")).Read(res).Run()
	if err != nil {
		t.Fatal(err)
	}

	expected := []struct {
		Tag string
		Id  string
	}{
		{"B", "1004"},
		{"B", "1001"},
		{"A", "1003"},
	}
	if len(*res) != len(expected) {
		t.Fatal("Expected", len(*res), " results, got", len(*res))
	}
	for i, e := range expected {
		result := (*res)[i]
		if result.Id != e.Id || result.Tag != e.Tag {
			t.Fatal("Got result out of order. i:", i, "expected ID:", e.Id, "actual ID:", result.Id, "expected tag:", e.Tag, "actual tag:", result.Tag)
		}
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
