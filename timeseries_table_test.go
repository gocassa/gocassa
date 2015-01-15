package cmagic

import (
	"testing"
	"time"
)

type Trip struct {
	Id string
	Time time.Time
}

func parse(value string) time.Time {
	t, err := time.Parse("2006 Jan 2 15:04:05", value)
	if err != nil {
		panic(err)
	}
	return t
}

func TestTimeSeriesTable(t *testing.T) {
	tbl := ns.TimeSeriesTable("tripTime", "Id", "Time", time.Minute, Trip{})
	createIf(ns, tbl.(*timeSeriesTable).t, t)
	err := tbl.Set(Trip{
		Id: "1",
		Time: parse("2006 Jan 2 15:03:59"),
	})
	if err != nil {
		t.Fatal(err)
	}
	err = tbl.Set(Trip{
		Id: "2",
		Time: parse("2006 Jan 2 15:04:00"),
	})
	if err != nil {
		t.Fatal(err)
	}
	err = tbl.Set(Trip{
		Id: "3",
		Time: parse("2006 Jan 2 15:04:01"),
	})
	if err != nil {
		t.Fatal(err)
	}
}