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
	test(func(k KeySpace) {
		tbl := k.MultiTimeSeriesTable("tripTime6", "Tag", "Time", "Id", time.Minute, TripB{})
		createIf(tbl.(TableChanger), t)
		err := tbl.WithOptions(Options{TTL: 30 * time.Second}).Set(TripB{
			Id:   "1",
			Time: parse("2006 Jan 2 15:03:59"),
			Tag:  "A",
		}).Add(tbl.Set(TripB{
			Id:   "2",
			Time: parse("2006 Jan 2 15:04:00"),
			Tag:  "B",
		})).Add(tbl.Set(TripB{
			Id:   "3",
			Time: parse("2006 Jan 2 15:04:01"),
			Tag:  "A",
		})).Add(tbl.Set(TripB{
			Id:   "4",
			Time: parse("2006 Jan 2 15:05:01"),
			Tag:  "B",
		})).Run()
		if err != nil {
			t.Fatal(err)
		}
		ts := &[]TripB{}
		err = tbl.List("A", parse("2006 Jan 2 15:03:58"), parse("2006 Jan 2 15:04:02"), ts).Run()
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
		err = tbl.List("B", parse("2006 Jan 2 15:03:58"), parse("2006 Jan 2 15:04:02"), ts).Run()
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
		err = tbl.List("B", parse("2006 Jan 2 15:03:58"), parse("2006 Jan 2 15:05:02"), ts).Run()
		if err != nil {
			t.Fatal(err)
		}
		if len(*ts) != 2 {
			t.Fatal(ts)
		}
		err = tbl.List("B", parse("2006 Jan 2 15:03:58"), parse("2006 Jan 2 15:05:00"), ts).Run()
		if err != nil {
			t.Fatal(err)
		}
		if len(*ts) != 1 {
			t.Fatal(ts)
		}
	})
}
