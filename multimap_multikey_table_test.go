package gocassa

import (
	"reflect"
	"testing"
)

type Store struct {
	City    string
	Manager string
	Id      string
	Address string
}

var (
	tablename  = "store"
	StoreIndex = []string{"Manager", "Id"}
	StorePK    = []string{"City"}
	CityKey    = StorePK[0]
	ManagerKey = StoreIndex[0]
	IdKey      = StoreIndex[1]
)

func TestMultimapMultiKeyTableInsertRead(t *testing.T) {
	tbl := ns.MultimapMultiKeyTable(tablename+"90", StorePK, StoreIndex, Store{})
	createIf(tbl.(TableChanger), t)
	london := Store{
		City:    "London",
		Manager: "Joe",
		Id:      "12412-afa-16956",
		Address: "Somerset House",
	}
	err := tbl.Set(london).Run()
	if err != nil {
		t.Fatal(err)
	}

	field := make(map[string]interface{})
	id := make(map[string]interface{})

	field[CityKey] = "London"
	id[ManagerKey] = "Joe"
	id[IdKey] = "12412-afa-16956"

	res := &Store{}
	err = tbl.Read(field, id, res).Run()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(*res, london) {
		t.Fatal(*res, london)
	}

	id[IdKey] = "12412"
	err = tbl.Read(field, id, res).Run()
	if err == nil {
		t.Fatal(*res)
	}
	id[IdKey] = "12412-afa-16956"
	err = tbl.Update(field, id, map[string]interface{}{
		"Address": "Soho",
	}).Run()
	if err != nil {
		t.Fatal(err)
	}
	err = tbl.Read(field, id, res).Run()
	if err != nil {
		t.Fatal(err)
	}
	if res.Address != "Soho" {
		t.Fatal(*res)
	}

	list := &[]Store{}
	err = tbl.List(field, nil, 20, list).Run()
	if err != nil {
		t.Fatal(err)
	}
	if len(*list) != 1 {
		t.Fatal(*list)
	}
}

func TestMultimapMultiKeyTableDelete(t *testing.T) {
	tbl := ns.MultimapMultiKeyTable(tablename+"91", StorePK, StoreIndex, Store{})
	createIf(tbl.(TableChanger), t)
	london := Store{
		City:    "London",
		Manager: "Joe",
		Id:      "12412-afa-16956",
		Address: "Somerset House",
	}
	err := tbl.Set(london).Run()
	if err != nil {
		t.Fatal(err)
	}
	field := make(map[string]interface{})
	id := make(map[string]interface{})

	field[CityKey] = "London"
	id[ManagerKey] = "Joe"
	id[IdKey] = "12412-afa-16956"

	res := &Store{}
	err = tbl.Read(field, id, res).Run()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(*res, london) {
		t.Fatal(*res, london)
	}
	err = tbl.Delete(field, id).Run()
	if err != nil {
		t.Fatal(err)
	}
	err = tbl.Read(field, id, res).Run()
	if err == nil {
		t.Fatal(res)
	}
}

func TestMultimapMultiKeyTableMultiRead(t *testing.T) {
	tbl := ns.MultimapMultiKeyTable(tablename+"92", StorePK, StoreIndex, Store{})
	createIf(tbl.(TableChanger), t)
	london_somer := Store{
		City:    "London",
		Manager: "Joe",
		Id:      "12412-afa-16956",
		Address: "Somerset House",
	}
	err := tbl.Set(london_somer).Run()
	if err != nil {
		t.Fatal(err)
	}
	london_soho := Store{
		City:    "London",
		Manager: "Joe",
		Id:      "12412-afa-16957",
		Address: "Soho",
	}
	err = tbl.Set(london_soho).Run()
	if err != nil {
		t.Fatal(err)
	}

	field := make(map[string]interface{})
	id := make(map[string]interface{})

	field[CityKey] = "London"
	id[ManagerKey] = "Joe"

	stores := &[]Store{}
	err = tbl.MultiRead(field, id, stores).Run()
	if err != nil {
		t.Fatal(err)
	}
	if len(*stores) != 2 {
		t.Fatalf("Expected to multiread 2 records, got %d", len(*stores))
	}
	if !reflect.DeepEqual((*stores)[0], london_somer) {
		t.Fatalf("Expected to find london_somer, got %v", (*stores)[0])
	}
	if !reflect.DeepEqual((*stores)[1], london_soho) {
		t.Fatalf("Expected to find london_soho, got %v", (*stores)[1])
	}
}
