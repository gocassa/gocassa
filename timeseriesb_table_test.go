package cmagic

import (
	"testing"
	"time"
)

type TripB struct {
	Id   string
	Time time.Time
	Tag  string
}

func TesttimeSeriesBT(t *testing.T) {
	tbl := ns.timeSeriesBTable("tripTime6", "Tag", "Time", "Id", time.Minute, TripB{})
	createIf(ns, tbl.(*timeSeriesBT).T, t)
	err := tbl.Set(TripB{
		Id:   "1",
		Time: parse("2006 Jan 2 15:03:59"),
		Tag:  "A",
	})
	if err != nil {
		t.Fatal(err)
	}
	err = tbl.Set(TripB{
		Id:   "2",
		Time: parse("2006 Jan 2 15:04:00"),
		Tag:  "B",
	})
	if err != nil {
		t.Fatal(err)
	}
	err = tbl.Set(TripB{
		Id:   "3",
		Time: parse("2006 Jan 2 15:04:01"),
		Tag:  "A",
	})
	if err != nil {
		t.Fatal(err)
	}
	err = tbl.Set(TripB{
		Id:   "4",
		Time: parse("2006 Jan 2 15:05:01"),
		Tag:  "B",
	})
	if err != nil {
		t.Fatal(err)
	}
	ts, err := tbl.List("A", parse("2006 Jan 2 15:03:58"), parse("2006 Jan 2 15:04:02"))
	if err != nil {
		t.Fatal(err)
	}
	if len(ts) != 2 {
		t.Fatal(ts)
	}
	if ts[0].(*TripB).Id != "1" || ts[1].(*TripB).Id != "3" {
		t.Fatal(ts[0].(*TripB), ts[1].(*TripB))
	}
	ts, err = tbl.List("B", parse("2006 Jan 2 15:03:58"), parse("2006 Jan 2 15:04:02"))
	if err != nil {
		t.Fatal(err)
	}
	if len(ts) != 1 {
		t.Fatal(ts)
	}
	if ts[0].(*TripB).Id != "2" {
		t.Fatal(ts[0].(*TripB))
	}
	ts, err = tbl.List("B", parse("2006 Jan 2 15:03:58"), parse("2006 Jan 2 15:05:02"))
	if err != nil {
		t.Fatal(err)
	}
	if len(ts) != 2 {
		t.Fatal(ts)
	}
	ts, err = tbl.List("B", parse("2006 Jan 2 15:03:58"), parse("2006 Jan 2 15:05:00"))
	if err != nil {
		t.Fatal(err)
	}
	if len(ts) != 1 {
		t.Fatal(ts)
	}
}
