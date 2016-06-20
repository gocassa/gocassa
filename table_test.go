package gocassa

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/gocql/gocql"
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
	randy := rand.Int() % 100
	name := fmt.Sprintf("customer_%v", randy)
	cs := ns.Table(name, Customer{}, Keys{
		PartitionKeys: []string{"Id", "Name"},
	})
	createIf(cs, t)
	validateTableName(t, cs.(TableChanger), fmt.Sprintf("customer_%d__Id_Name__", randy))
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
	if err := ns.(*k).TruncateTable(cs.Name()); err != nil {
		t.Errorf("Failed to truncate table: %s error: %s", cs.Name(), err)
	}
	res2 := &[]Customer{}
	if err := cs.Where(Eq("Id", "1001"), Eq("Name", "Joe")).Read(res2).Run(); err != nil {
		t.Errorf("Failed to read table: %s error: %s", cs.Name(), err)
	}
	if len(*res2) != 0 {
		t.Errorf("Expected table to be empty, but found: %+v", res2)
	}
	err = ns.(*k).DropTable(name)
	if err != nil {
		t.Fatal(err)
	}
}

func TestClusteringOrder(t *testing.T) {
	options := Options{}.AppendClusteringOrder("Id", DESC)
	name := "customer_by_name"
	cs := ns.Table(name, Customer{}, Keys{
		PartitionKeys:     []string{"Name"},
		ClusteringColumns: []string{"Id"},
	}).WithOptions(options)
	createIf(cs, t)
	validateTableName(t, cs.(TableChanger), "customer_by_name__Name__Id")

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
	options := Options{}.AppendClusteringOrder("Tag", DESC).AppendClusteringOrder("Id", DESC)
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
	validateTableName(t, cs.(TableChanger), "something__Id_Name__")
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

func TestAllowFiltering(t *testing.T) {
	name := "allow_filtering"
	cs := ns.Table(name, Customer2{}, Keys{
		PartitionKeys:     []string{"Name"},
		ClusteringColumns: []string{"Tag", "Id"},
	})
	createIf(cs, t)
	c2 := Customer2{}
	//This shouldn't contain allow filtering
	st, _ := cs.Where(Eq("Name", "Brian")).Read(&c2).GenerateStatement()
	if strings.Contains(st, "ALLOW FILTERING") {
		t.Error("Allow filtering should be disabled by default")
	}

	op := Options{AllowFiltering: true}
	stAllow, _ := cs.Where(Eq("", "")).Read(&c2).WithOptions(op).GenerateStatement()
	if !strings.Contains(stAllow, "ALLOW FILTERING") {
		t.Error("Allow filtering show be included in the statement")
	}
}

func TestKeysCreation(t *testing.T) {
	cs := ns.Table("composite_keys", Customer{}, Keys{
		PartitionKeys: []string{"Id", "Name"},
	})
	str, err := cs.CreateStatement()
	if err != nil {
		t.Fatal(err)
	}
	//composite
	if !strings.Contains(str, "PRIMARY KEY ((id, name ))") {
		t.Fatal(str)
	}

	cs = ns.Table("compound_keys", Customer{}, Keys{
		PartitionKeys: []string{"Id", "Name"},
		Compound:      true,
	})
	str, err = cs.CreateStatement()
	if err != nil {
		t.Fatal(err)
	}
	//compound
	if !strings.Contains(str, "PRIMARY KEY (id, name )") {
		t.Fatal(str)
	}

	cs = ns.Table("clustering_keys", Customer{}, Keys{
		PartitionKeys:     []string{"Id"},
		ClusteringColumns: []string{"Name"},
	})
	str, err = cs.CreateStatement()
	if err != nil {
		t.Fatal(err)
	}
	//with columns
	if !strings.Contains(str, "PRIMARY KEY ((id), name)") {
		t.Fatal(str)
	}
	//compound gets ignored when using clustering columns
	cs = ns.Table("clustering_keys", Customer{}, Keys{
		PartitionKeys:     []string{"Id"},
		ClusteringColumns: []string{"Name"},
		Compound:          true,
	})
	str, err = cs.CreateStatement()
	if err != nil {
		t.Fatal(err)
	}
	//with columns
	if !strings.Contains(str, "PRIMARY KEY ((id), name)") {
		t.Fatal(str)
	}
}

// Mock QueryExecutor that keeps track of options passed to it
type OptionCheckingQE struct {
	opts *Options
}

func (qe OptionCheckingQE) QueryWithOptions(opts Options, stmt string, params ...interface{}) ([]map[string]interface{}, error) {
	qe.opts.Consistency = opts.Consistency
	return []map[string]interface{}{}, nil
}

func (qe OptionCheckingQE) Query(stmt string, params ...interface{}) ([]map[string]interface{}, error) {
	return qe.QueryWithOptions(Options{}, stmt, params...)
}

func (qe OptionCheckingQE) ExecuteWithOptions(opts Options, stmt string, params ...interface{}) error {
	qe.opts.Consistency = opts.Consistency
	return nil
}

func (qe OptionCheckingQE) Execute(stmt string, params ...interface{}) error {
	return qe.ExecuteWithOptions(Options{}, stmt, params...)
}

func (qe OptionCheckingQE) ExecuteAtomically(stmt []string, params [][]interface{}) error {
	return nil
}

func (qe OptionCheckingQE) Close() {
}

func TestQueryWithConsistency(t *testing.T) {
	// It's tricky to verify this against a live DB, so mock out the
	// query executor and make sure the right options get passed
	// through
	resultOpts := Options{}
	qe := OptionCheckingQE{opts: &resultOpts}
	conn := &connection{q: qe}
	ks := conn.KeySpace("some ks")
	cs := ks.Table("customerWithConsistency", Customer{}, Keys{PartitionKeys: []string{"Id"}})
	res := &[]Customer{}
	cons := gocql.Quorum
	opts := Options{Consistency: &cons}

	q := cs.Where(Eq("Id", 1)).Read(res).WithOptions(opts)

	if err := q.Run(); err != nil {
		t.Fatal(err)
	}
	if resultOpts.Consistency == nil {
		t.Fatal(fmt.Sprint("Expected consistency:", cons, "got: nil"))
	}
	if resultOpts.Consistency != nil && *resultOpts.Consistency != cons {
		t.Fatal(fmt.Sprint("Expected consistency:", cons, "got:", resultOpts.Consistency))
	}
}

func TestExecuteWithConsistency(t *testing.T) {
	resultOpts := Options{}
	qe := OptionCheckingQE{opts: &resultOpts}
	conn := &connection{q: qe}
	ks := conn.KeySpace("some ks")
	cs := ks.Table("customerWithConsistency2", Customer{}, Keys{PartitionKeys: []string{"Id"}})
	cons := gocql.All
	opts := Options{Consistency: &cons}

	// This calls Execute() under the covers
	err := cs.Set(Customer{
		Id:   "100",
		Name: "Joe",
	}).WithOptions(opts).Run()
	if err != nil {
		t.Fatal(err)
	}
	if resultOpts.Consistency == nil {
		t.Fatal(fmt.Sprint("Expected consistency:", cons, "got: nil"))
	}
	if resultOpts.Consistency != nil && *resultOpts.Consistency != cons {
		t.Fatal(fmt.Sprint("Expected consistency:", cons, "got:", resultOpts.Consistency))
	}
}

func validateTableName(t *testing.T, tbl TableChanger, expected string) bool {
	ok := tbl.Name() == expected
	if !ok {
		t.Fatalf("Table name should be: %s and NOT: %s", expected, tbl.Name())
	}
	return ok
}
