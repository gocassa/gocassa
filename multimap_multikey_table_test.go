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
	validateTableName(t, tbl.(TableChanger), "store90_multimapMk")
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

	for i, s := range []Store{
		{City: "London", Manager: "Joe", Id: "12412-afa-16961", Address: "Westminster"},
		{City: "Anstruther", Manager: "Morag", Id: "12412-afa-16963", Address: "Anstruther"},
	} {
		err = tbl.Set(s).Run()
		if err != nil {
			t.Fatalf("Add store [%d]: error: %s", i, err)
		}
	}
	field[CityKey] = "London"
	id[ManagerKey] = "Joe"
	id[IdKey] = []string{"12412-afa-16956", "12412-afa-16961"}

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
	if (*stores)[1].Address != "Westminster" {
		t.Fatalf("Expected to find joe in Westminster, got %v", (*stores)[1])
	}
}

func TestMultimapMultiKeyTableListOrder(t *testing.T) {
	tbl := ns.MultimapMultiKeyTable(tablename+"93", StorePK, StoreIndex, Store{})
	createIf(tbl.(TableChanger), t)
	store1 := Store{
		City:    "London",
		Manager: "Joe",
		Id:      "12412-afa-16955",
		Address: "Somerset House",
	}
	err := tbl.Set(store1).Run()
	if err != nil {
		t.Fatal(err)
	}
	store2 := Store{
		City:    "London",
		Manager: "Joe",
		Id:      "12412-afa-16956",
		Address: "Waterloo",
	}
	err = tbl.Set(store2).Run()
	if err != nil {
		t.Fatal(err)
	}
	store3 := Store{
		City:    "London",
		Manager: "Jane",
		Id:      "12412-afa-16957",
		Address: "Charing Cross",
	}
	err = tbl.Set(store3).Run()
	if err != nil {
		t.Fatal(err)
	}

	list := []Store{}
	op := tbl.List(map[string]interface{}{"City": "London"}, nil, 20, &list).WithOptions(Options{
		ClusteringOrder: []ClusteringOrderColumn{
			{DESC, "Manager"},
			{DESC, "Id"},
		},
	})

	err = op.Run()
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 3 {
		t.Fatalf("Expected to list 3 records, got %d", len(list))
	}
	if !reflect.DeepEqual(list[0], store2) {
		t.Fatalf("Expected to find waterloo, got %v", list[0].Address)
	}
	if !reflect.DeepEqual(list[1], store1) {
		t.Fatalf("Expected to find somerset house, got %v", list[1].Address)
	}
	if !reflect.DeepEqual(list[2], store3) {
		t.Fatalf("Expected to find charing cross, got %v", list[2].Address)
	}
}

func TestCruft(t *testing.T) {
	m := map[string]interface{}{
		"single":   "unitary",
		"multiple": []string{"one", "two"},
	}
	for k, v := range m {
		switch tt := v.(type) {
		case []string:
			if k != "multiple" {
				t.Errorf("Misidentified type (%T) for %s => %+v", tt, k, v)
			}
		case string:
			if k != "single" {
				t.Errorf("Misidentified type (%T) for %s => %+v", tt, k, v)
			}
		default:
			t.Errorf("How did we get here for type (%T) for %s => %+v", tt, k, v)
		}
		vv := reflect.ValueOf(v)
		if vv.Kind() == reflect.Array || vv.Kind() == reflect.Slice {
			if k != "multiple" {
				t.Errorf("R Misidentified type (%T) for %s => %+v", v, k, v)
			}
			if vv.Len() != 2 {
				t.Errorf("R Wrong length: %d", vv.Len())
			}
		} else {
			if k != "single" {
				t.Errorf("R Misidentified type (%T) for %s => %+v", v, k, v)
			}
		}
	}
}
