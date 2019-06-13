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
	options := Options{}.AppendClusteringOrder("Id", DESC)
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
	stmt, err := cs.CreateStatement()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stmt.Query(), "something") {
		t.Fatal(stmt.Query())
	}
	stmt, err = cs.WithOptions(Options{TableName: "funky"}).CreateStatement()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stmt.Query(), "funky") {
		t.Fatal(stmt.Query())
	}
	// if clustering order is not specified, it should omit the clustering order by and use the default
	if strings.Contains(stmt.Query(), "WITH CLUSTERING ORDER BY") {
		t.Fatal(stmt.Query())
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
	st := cs.Where(Eq("Name", "Brian")).Read(&c2).GenerateStatement()
	if strings.Contains(st.Query(), "ALLOW FILTERING") {
		t.Error("Allow filtering should be disabled by default")
	}

	op := Options{AllowFiltering: true}
	stAllow := cs.Where(Eq("", "")).Read(&c2).WithOptions(op).GenerateStatement()
	if !strings.Contains(stAllow.Query(), "ALLOW FILTERING") {
		t.Error("Allow filtering show be included in the statement")
	}
}

func TestKeysCreation(t *testing.T) {
	cs := ns.Table("composite_keys", Customer{}, Keys{
		PartitionKeys: []string{"Id", "Name"},
	})
	stmt, err := cs.CreateStatement()
	if err != nil {
		t.Fatal(err)
	}
	//composite
	if !strings.Contains(stmt.Query(), "PRIMARY KEY ((id, name ))") {
		t.Fatal(stmt.Query())
	}

	cs = ns.Table("compound_keys", Customer{}, Keys{
		PartitionKeys: []string{"Id", "Name"},
		Compound:      true,
	})
	stmt, err = cs.CreateStatement()
	if err != nil {
		t.Fatal(err)
	}
	//compound
	if !strings.Contains(stmt.Query(), "PRIMARY KEY (id, name )") {
		t.Fatal(stmt.Query())
	}

	cs = ns.Table("clustering_keys", Customer{}, Keys{
		PartitionKeys:     []string{"Id"},
		ClusteringColumns: []string{"Name"},
	})
	stmt, err = cs.CreateStatement()
	if err != nil {
		t.Fatal(err)
	}
	//with columns
	if !strings.Contains(stmt.Query(), "PRIMARY KEY ((id), name)") {
		t.Fatal(stmt.Query())
	}
	//compound gets ignored when using clustering columns
	cs = ns.Table("clustering_keys", Customer{}, Keys{
		PartitionKeys:     []string{"Id"},
		ClusteringColumns: []string{"Name"},
		Compound:          true,
	})
	stmt, err = cs.CreateStatement()
	if err != nil {
		t.Fatal(err)
	}
	//with columns
	if !strings.Contains(stmt.Query(), "PRIMARY KEY ((id), name)") {
		t.Fatal(stmt.Query())
	}
}

// Mock QueryExecutor that keeps track of options passed to it
type OptionCheckingQE struct {
	opts *Options
}

func (qe OptionCheckingQE) QueryWithOptions(opts Options, stmt Statement) ([]map[string]interface{}, error) {
	qe.opts.Consistency = opts.Consistency
	return []map[string]interface{}{}, nil
}

func (qe OptionCheckingQE) Query(stmt Statement) ([]map[string]interface{}, error) {
	return qe.QueryWithOptions(Options{}, stmt)
}

func (qe OptionCheckingQE) ExecuteWithOptions(opts Options, stmt Statement) error {
	qe.opts.Consistency = opts.Consistency
	return nil
}

func (qe OptionCheckingQE) Execute(stmt Statement) error {
	return qe.ExecuteWithOptions(Options{}, stmt)
}

func (qe OptionCheckingQE) ExecuteAtomically(stmt []Statement) error {
	return nil
}

func (qe OptionCheckingQE) ExecuteAtomicallyWithOptions(opts Options, stmt []Statement) error {
	qe.opts.Consistency = opts.Consistency
	return nil
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
