package gocassa

import (
	"testing"
	"time"
)

type Trip struct {
	Id   string
	Time time.Time
}

func parse(value string) time.Time {
	t, err := time.Parse("2006 Jan 2 15:04:05", value)
	if err != nil {
		panic(err)
	}
	return t
}

func TestTimeSeriesT(t *testing.T) {
	tbl := ns.TimeSeriesTable("tripTime5", "Time", "Id", time.Minute, Trip{})
	createIf(tbl.(TableChanger), t)
	err := tbl.Set(Trip{
		Id:   "1",
		Time: parse("2006 Jan 2 15:03:59"),
	})
	if err != nil {
		t.Fatal(err)
	}
	err = tbl.Set(Trip{
		Id:   "2",
		Time: parse("2006 Jan 2 15:04:00"),
	})
	if err != nil {
		t.Fatal(err)
	}
	err = tbl.Set(Trip{
		Id:   "3",
		Time: parse("2006 Jan 2 15:04:01"),
	})
	if err != nil {
		t.Fatal(err)
	}
	err = tbl.Set(Trip{
		Id:   "4",
		Time: parse("2006 Jan 2 15:05:01"),
	})
	if err != nil {
		t.Fatal(err)
	}
	ts := &[]Trip{}
	err = tbl.List(parse("2006 Jan 2 15:03:58"), parse("2006 Jan 2 15:04:02"), ts)
	if err != nil {
		t.Fatal(err)
	}
	if len(*ts) != 3 {
		t.Fatal(ts)
	}
	ts1 := *ts
	if ts1[0].Id != "1" || ts1[1].Id != "2" || ts1[2].Id != "3" {
		t.Fatal(ts1[0], ts1[1], ts1[2])
	}
	err = tbl.List(parse("2006 Jan 2 15:03:58"), parse("2006 Jan 2 15:05:02"), ts)
	if err != nil {
		t.Fatal(err)
	}
	if len(*ts) != 4 {
		t.Fatal(ts)
	}
}
