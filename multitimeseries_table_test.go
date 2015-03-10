package gocassa

import (
	"testing"
	"time"
)

type TripB struct {
	Id   string
	Time time.Time
	Tag  string
}

func TestMultiTimeSeriesTable(t *testing.T) {
	tbl := ns.MultiTimeSeriesTable("tripTime6", "Tag", "Time", "Id", time.Minute, TripB{})
	createIf(tbl.(TableChanger), t)
	err := Check(
		tbl.Set(TripB{
			Id:   "1",
			Time: parse("2006 Jan 2 15:03:59"),
			Tag:  "A",
		}),
		tbl.Set(TripB{
			Id:   "2",
			Time: parse("2006 Jan 2 15:04:00"),
			Tag:  "B",
		}),
		tbl.Set(TripB{
			Id:   "3",
			Time: parse("2006 Jan 2 15:04:01"),
			Tag:  "A",
		}),
		tbl.Set(TripB{
			Id:   "4",
			Time: parse("2006 Jan 2 15:05:01"),
			Tag:  "B",
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	ts := &[]TripB{}
	err = tbl.List("A", parse("2006 Jan 2 15:03:58"), parse("2006 Jan 2 15:04:02"), ts)
	if err != nil {
		t.Fatal(err)
	}
	if len(*ts) != 2 {
		t.Fatal(ts)
	}
	ts1 := *ts
	if ts1[0].Id != "1" || ts1[1].Id != "3" {
		t.Fatal(ts1[0], ts1[1])
	}
	err = tbl.List("B", parse("2006 Jan 2 15:03:58"), parse("2006 Jan 2 15:04:02"), ts)
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
	err = tbl.List("B", parse("2006 Jan 2 15:03:58"), parse("2006 Jan 2 15:05:02"), ts)
	if err != nil {
		t.Fatal(err)
	}
	if len(*ts) != 2 {
		t.Fatal(ts)
	}
	err = tbl.List("B", parse("2006 Jan 2 15:03:58"), parse("2006 Jan 2 15:05:00"), ts)
	if err != nil {
		t.Fatal(err)
	}
	if len(*ts) != 1 {
		t.Fatal(ts)
	}
}
