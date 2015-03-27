package gocassa

import (
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
)

type Customer struct {
	Id   string
	Name string
}

var ns KeySpace

func getTestHosts() []string {
	if h := os.Getenv("GOCASSA_TEST_HOSTS"); h != "" {
		return strings.Split(h, ",")
	} else {
		return []string{"127.0.0.1"}
	}
}

func init() {
	kname := "test_ihopeudonthaveakeyspacenamedlikedthis"
	var err error

	c, err := Connect(getTestHosts(), "", "")
	if err != nil {
		panic(err)
	}
	err = c.DropKeySpace(kname)
	if err != nil {
		panic(err)
	}
	err = c.CreateKeySpace(kname)
	if err != nil {
		panic(err)
	}
	ns, err = ConnectToKeySpace(kname, getTestHosts(), "", "")
	if err != nil {
		panic(err)
	}
	//ns.DebugMode(true)
}

func TestEq(t *testing.T) {
	cs := ns.Table("customer", Customer{}, Keys{PartitionKeys: []string{"Id"}})
	createIf(cs.(TableChanger), t)
	err := cs.Set(Customer{
		Id:   "50",
		Name: "Joe",
	}).Run()
	if err != nil {
		t.Fatal(err)
	}
	res := &[]Customer{}
	err = cs.Where(Eq("Id", "50")).Query().Read(res).Run()
	if err != nil {
		t.Fatal(err)
	}
	if len(*res) == 0 {
		t.Fatal("Not found")
	}
	if (*res)[0].Id != "50" {
		t.Fatal((*res)[0])
	}
}

func TestMultipleRowResults(t *testing.T) {
	name := "customer_multipletest"
	cs := ns.Table(name, Customer{}, Keys{
		PartitionKeys:     []string{"Name"},
		ClusteringColumns: []string{"Id"},
	})
	err := cs.(TableChanger).Recreate()
	if err != nil {
		t.Fatal(err)
	}
	err = cs.Set(Customer{
		Id:   "12",
		Name: "John",
	}).Add(cs.Set(Customer{
		Id:   "13",
		Name: "John",
	})).Run()
	if err != nil {
		t.Fatal(err)
	}

	res := []Customer{}
	err = cs.Where(Eq("Name", "John")).Query().Read(&res).Run()
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 2 {
		t.Fatal(res)
	}
}

func TestIn(t *testing.T) {
	cs := ns.Table("customer", Customer{}, Keys{
		PartitionKeys: []string{"Id"},
	})
	err := cs.Set(Customer{
		Id:   "100",
		Name: "Joe",
	}).Add(cs.Set(Customer{
		Id:   "200",
		Name: "Jane",
	})).Run()
	if err != nil {
		t.Fatal(err)
	}
	res := []Customer{}
	err = cs.Where(In("Id", "100", "200")).Query().Read(&res).Run()
	if len(res) != 2 {
		for _, v := range res {
			fmt.Println(v)
		}
		t.Fatal("Not found", res)
	}
	if res[0].Id != "100" || res[1].Id != "200" {
		t.Fatal(res[0], res[1])
	}
}

func TestAnd(t *testing.T) {
	cs := ns.Table("customer1", Customer{}, Keys{PartitionKeys: []string{"Id", "Name"}})
	createIf(cs.(TableChanger), t)
	err := cs.Set(Customer{
		Id:   "100",
		Name: "Joe",
	}).Run()
	if err != nil {
		t.Fatal(err)
	}
	res := []Customer{}
	err = cs.Where(Eq("Id", "100"), Eq("Name", "Joe")).Query().Read(&res).Run()
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 1 {
		t.Fatal("Not found ", res)
	}
}

func TestQueryReturnError(t *testing.T) {
	cs := ns.Table("customer2", Customer{}, Keys{})
	res := []Customer{}
	err := cs.Where(Eq("Id", "100"), Eq("Name", "Joe")).Query().Read(&res).Run()
	if err == nil {
		t.Fatal("Table customer2 does not exist - should return error")
	}
}

func TestRowNotFoundError(t *testing.T) {
	cs := ns.MapTable("customer", "Id", Customer{})
	createIf(cs.(TableChanger), t)
	c := &Customer{}
	err := cs.Read("8sad8as8ds8u34", c).Run()
	_, ok := err.(RowNotFoundError)
	if !ok {
		t.Fatal(err)
	}
}

type Customer3 struct {
	Id       string
	String   string
	Int      int
	Int32    int32
	Int64    int64
	Float32  float32
	Float64  float64
	Bool     bool
	Bytes    []byte
	Strings  []string
	Ints     []int
	Int32s   []int32
	Int64s   []int64
	Float32s []float32
	Float64s []float64
	Bools    []bool
}

func newCustomer3() Customer3 {
	return Customer3{
		Id:       "1",
		String:   "A",
		Int:      1,
		Int32:    2,
		Int64:    3,
		Float32:  4.0,
		Float64:  5.0,
		Bool:     true,
		Bytes:    []byte{'a', 'b', 'c'},
		Strings:  []string{"a", "b", "c"},
		Ints:     []int{1, 2, 3},
		Int32s:   []int32{1, 2, 3},
		Int64s:   []int64{1, 2, 3},
		Float32s: []float32{1.11, 2.22, 3.33},
		Float64s: []float64{1.11, 2.22, 3.33},
		Bools:    []bool{false, true, false},
	}
}

func TestTypesMarshal(t *testing.T) {
	c := newCustomer3()
	tbl := ns.Table("customer3", Customer3{}, Keys{PartitionKeys: []string{"Id"}})
	createIf(tbl.(TableChanger), t)
	if err := tbl.Set(c).Run(); err != nil {
		t.Fatal(err)
	}
	res := []Customer3{}
	err := tbl.Where(Eq("Id", "1")).Query().Read(&res).Run()
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 1 {
		t.Fatal(res)
	}
	if !reflect.DeepEqual(c, res[0]) {
		t.Fatal(c, res[0])
	}
}

func TestUpdateList(t *testing.T) {
	tbl := ns.Table("customer34", Customer3{}, Keys{PartitionKeys: []string{"Id"}})
	createIf(tbl.(TableChanger), t)
	c := newCustomer3()
	if err := tbl.Set(c).Run(); err != nil {
		t.Fatal(err)
	}
	f := tbl.Where(Eq("Id", "1"))
	err := f.Update(map[string]interface{}{
		"Strings":  ListRemove("b"),
		"Ints":     ListRemove(2),
		"Int32s":   ListRemove(2),
		"Int64s":   ListRemove(2),
		"Float32s": ListRemove(2.22),
		"Float64s": ListRemove(2.22),
		"Bools":    ListRemove(true),
	}).Run()
	if err != nil {
		t.Fatal(err)
	}
	err = f.Query().ReadOne(&c).Run()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(c.Strings, []string{"a", "c"}) ||
		!reflect.DeepEqual(c.Ints, []int{1, 3}) ||
		!reflect.DeepEqual(c.Int32s, []int32{1, 3}) ||
		!reflect.DeepEqual(c.Int64s, []int64{1, 3}) ||
		!reflect.DeepEqual(c.Float32s, []float32{1.11, 3.33}) ||
		!reflect.DeepEqual(c.Float64s, []float64{1.11, 3.33}) ||
		!reflect.DeepEqual(c.Bools, []bool{false, false}) {
		t.Fatal(c)
	}
	err = f.Update(map[string]interface{}{
		"Strings": ListSetAtIndex(1, "C"),
	}).Run()
	if err != nil {
		t.Fatal(err)
	}
	err = f.Query().ReadOne(&c).Run()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(c.Strings, []string{"a", "C"}) {
		t.Fatal(c)
	}
	err = f.Update(map[string]interface{}{
		"Strings": ListAppend("d"),
	}).Run()
	if err != nil {
		t.Fatal(err)
	}
	err = f.Query().ReadOne(&c).Run()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(c.Strings, []string{"a", "C", "d"}) {
		t.Fatal(c)
	}
	err = f.Update(map[string]interface{}{
		"Strings": ListPrepend("x"),
	}).Run()
	if err != nil {
		t.Fatal(err)
	}
	err = f.Query().ReadOne(&c).Run()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(c.Strings, []string{"x", "a", "C", "d"}) {
		t.Fatal(c)
	}
}

func TestCQLInjection(t *testing.T) {
	tbl := ns.Table("customer45", Customer3{}, Keys{PartitionKeys: []string{"Id"}})
	createIf(tbl.(TableChanger), t)
	c := Customer3{
		Id:      "1",
		Strings: []string{"a", "b", "c"},
	}
	if err := tbl.Set(c).Run(); err != nil {
		t.Fatal(err)
	}
	// At the moment we don't have batch so we just try to mess up the CQL query with a single quote - if
	// it can be messed up then we are vulnerable
	err := tbl.Where(Eq("Id", "1")).Update(map[string]interface{}{
		"Strings": ListRemove("'"),
	}).Run()
	if err != nil {
		t.Fatal(err)
	}
}

type CustomerWithMap struct {
	Id  string
	Map map[string]string
}

func TestMaps(t *testing.T) {
	tbl := ns.MapTable("customer34213", "Id", CustomerWithMap{})
	createIf(tbl.(TableChanger), t)
	c := CustomerWithMap{
		Id: "1",
		Map: map[string]string{
			"3": "Is Odd",
			"6": "Is Even",
		},
	}
	if err := tbl.Set(c).Run(); err != nil {
		t.Fatal(err)
	}
	if err := tbl.Update("1", map[string]interface{}{
		"Map": MapSetFields(map[string]interface{}{
			"2": "Two",
			"4": "Four",
		}),
	}).Add(tbl.Update("1", map[string]interface{}{
		"Map": MapSetField("5", "Five!"),
	})).Run(); err != nil {
		t.Fatal(err)
	}

	// Read back into a new struct (see #83)
	var c2 CustomerWithMap
	if err := tbl.Read("1", &c2).Run(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(c2, CustomerWithMap{
		Id: "1",
		Map: map[string]string{
			"2": "Two",
			"3": "Is Odd",
			"4": "Four",
			"5": "Five!",
			"6": "Is Even",
		},
	}) {
		t.Fatal(c2)
	}
}

type CustomerWithCounter struct {
	Id      string
	Counter Counter
}

func TestCounters(t *testing.T) {
	tbl := ns.MapTable("customer4985", "Id", CustomerWithCounter{})
	createIf(tbl.(TableChanger), t)
	c := CustomerWithCounter{
		Id:      "1",
		Counter: Counter(0),
	}
	if err := tbl.Set(c).Run(); err != nil {
		t.Fatal(err)
	}
	if err := tbl.Update("1", map[string]interface{}{
		"Counter": CounterIncrement(6),
	}).Run(); err != nil {
		t.Fatal(err)
	}
	if err := tbl.Read("1", &c).Run(); err != nil {
		t.Fatal(err)
	}
	if c.Counter != Counter(6) {
		t.Fatal(c)
	}
	if err := tbl.Update("1", map[string]interface{}{
		"Counter": CounterIncrement(-2),
	}).Run(); err != nil {
		t.Fatal(err)
	}
	if err := tbl.Read("1", &c).Run(); err != nil {
		t.Fatal(err)
	}
	if c.Counter != Counter(4) {
		t.Fatal(c)
	}
}
