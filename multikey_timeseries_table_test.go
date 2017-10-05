package gocassa

import (
	"testing"
	"time"
)

type TripC struct {
	Id   string
	Time time.Time
	TagA string
	TagB string
}

func TestMultiKeyTimeSeriesTable(t *testing.T) {
	tbl := ns.MultiKeyTimeSeriesTable(
		"tripTime6",
		[]string{"TagA", "TagB"},
		"Time",
		[]string{"Id"},
		time.Minute,
		TripC{},
	)
	createIf(tbl.(TableChanger), t)
	err := tbl.WithOptions(Options{TTL: 30 * time.Second}).Set(TripC{
		Id:   "1",
		Time: parse("2006 Jan 2 15:03:59"),
		TagA: "A",
		TagB: "A",
	}).Add(tbl.Set(TripC{
		Id:   "2",
		Time: parse("2006 Jan 2 15:04:00"),
		TagA: "B",
		TagB: "B",
	})).Add(tbl.Set(TripC{
		Id:   "3",
		Time: parse("2006 Jan 2 15:04:01"),
		TagA: "A",
		TagB: "B",
	})).Add(tbl.Set(TripC{
		Id:   "4",
		Time: parse("2006 Jan 2 15:05:01"),
		TagA: "B",
		TagB: "B",
	})).Run()
	if err != nil {
		t.Fatal(err)
	}
	ts := &[]TripC{}
	err = tbl.List(map[string]interface{}{
		"TagA": "A",
		"TagB": "A",
	}, parse("2006 Jan 2 15:03:58"), parse("2006 Jan 2 15:04:02"), ts).Run()
	if err != nil {
		t.Fatal(err)
	}
	if len(*ts) != 1 {
		t.Fatal(ts)
	}
	ts1 := *ts
	if ts1[0].Id != "1" {
		t.Fatal(ts1[0])
	}
	err = tbl.List(map[string]interface{}{
		"TagA": "B",
		"TagB": "B",
	}, parse("2006 Jan 2 15:03:58"), parse("2006 Jan 2 15:04:02"), ts).Run()
	if err != nil {
		t.Fatal(err)
	}
	if len(*ts) != 1 {
		t.Fatal(ts)
	}
	ts1 = *ts
	if ts1[0].Id != "2" {
		t.Fatal(ts1[0])
	}
	err = tbl.List(map[string]interface{}{
		"TagA": "B",
		"TagB": "B",
	}, parse("2006 Jan 2 15:03:58"), parse("2006 Jan 2 15:05:02"), ts).Run()
	if err != nil {
		t.Fatal(err)
	}
	if len(*ts) != 2 {
		t.Fatal(ts)
	}
	err = tbl.List(map[string]interface{}{
		"TagA": "B",
		"TagB": "B",
	}, parse("2006 Jan 2 15:03:58"), parse("2006 Jan 2 15:05:00"), ts).Run()
	if err != nil {
		t.Fatal(err)
	}
	if len(*ts) != 1 {
		t.Fatal(ts)
	}

	trip := &TripC{}
	err = tbl.Read(map[string]interface{}{
		"TagA": "A",
		"TagB": "B",
	}, parse("2006 Jan 2 15:04:01"), map[string]interface{}{
		"Id": "3",
	}, trip).Run()
	if err != nil {
		t.Fatal(err)
	}
	if trip.Id != "3" {
		t.Fatal(ts)
	}
}
