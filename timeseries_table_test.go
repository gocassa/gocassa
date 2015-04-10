package gocassa

import (
	"fmt"
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
	}).Add(tbl.Set(Trip{
		Id:   "2",
		Time: parse("2006 Jan 2 15:04:00"),
	})).Add(tbl.Set(Trip{
		Id:   "3",
		Time: parse("2006 Jan 2 15:04:01"),
	})).Add(tbl.Set(Trip{
		Id:   "4",
		Time: parse("2006 Jan 2 15:05:01"),
	})).Run()
	if err != nil {
		t.Fatal(err)
	}
	ts := &[]Trip{}
	err = tbl.List(parse("2006 Jan 2 15:03:58"), parse("2006 Jan 2 15:04:02"), ts).Run()
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
	err = tbl.List(parse("2006 Jan 2 15:03:58"), parse("2006 Jan 2 15:05:02"), ts).Run()
	if err != nil {
		t.Fatal(err)
	}
	if len(*ts) != 4 {
		t.Fatal(ts)
	}
}

func TestOptions(t *testing.T) {
	tbl := ns.TimeSeriesTable("tripTime6", "Time", "Id", time.Hour, Trip{})
	createIf(tbl.(TableChanger), t)
	for i := 0; i < 10; i++ {
		i := Trip{
			Id:   fmt.Sprintf("%v", i),
			Time: time.Now(),
		}
		if err := tbl.Set(i).Run(); err != nil {
			t.Fatal(err)
		}
	}
	res := []Trip{}
	err := tbl.List(time.Now().Add(-1*time.Hour), time.Now().Add(1*time.Hour), &res).Run()
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 10 {
		t.Fatal(len(res))
	}
	err = tbl.WithOptions(Options{Limit: 3}).List(time.Now().Add(-1*time.Hour), time.Now().Add(1*time.Hour), &res).Run()
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 3 {
		t.Fatal(len(res))
	}
	res1 := []Trip{}
	err = tbl.List(time.Now().Add(-1*time.Hour), time.Now().Add(1*time.Hour), &res1).WithOptions(Options{Limit: 2}).Run()
	if err != nil {
		t.Fatal(err)
	}
	if len(res1) != 2 {
		t.Fatal(res1)
	}
	listOp := tbl.List(time.Now().Add(-1*time.Hour), time.Now().Add(1*time.Hour), &res)
	listOp1 := tbl.List(time.Now().Add(-1*time.Hour), time.Now().Add(1*time.Hour), &res1)
	err = listOp.WithOptions(Options{Limit: 4}).Add(listOp1.WithOptions(Options{Limit: 5})).Run()
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 4 || len(res1) != 5 {
		t.Fatal(res, res1)
	}
	err = listOp.WithOptions(Options{Limit: 4}).Add(listOp1.WithOptions(Options{Limit: 5})).WithOptions(Options{Limit: 6}).Run()
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 6 || len(res1) != 6 {
		t.Fatal(err)
	}
}
