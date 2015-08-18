package gocassa

import (
	"testing"
	"time"
)

func TestMultiFlakeSeriesT(t *testing.T) {
	tbl := ns.MultiFlakeSeriesTable("tripFlake6", "Tag", "Id", time.Minute, TripB{})
	createIf(tbl.(TableChanger), t)
	id1 := timeToFlake(t, "2006 Jan 2 15:03:59")
	id2 := timeToFlake(t, "2006 Jan 2 15:04:00")
	id3 := timeToFlake(t, "2006 Jan 2 15:04:01")
	id4 := timeToFlake(t, "2006 Jan 2 15:05:01")

	err := tbl.WithOptions(Options{TTL: 30 * time.Second}).Set(TripB{
		Id:  id1,
		Tag: "A",
	}).Add(tbl.Set(TripB{
		Id:  id2,
		Tag: "B",
	})).Add(tbl.Set(TripB{
		Id:  id3,
		Tag: "A",
	})).Add(tbl.Set(TripB{
		Id:  id4,
		Tag: "B",
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
	if ts1[0].Id != id1 || ts1[1].Id != id3 {
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
	if ts1[0].Id != id2 {
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
}
