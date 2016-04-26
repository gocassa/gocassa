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

func TestPagingTimeSeries(t *testing.T) {
	tbl := ns.TimeSeriesTable("tripTime7", "Time", "Id", time.Hour, Trip{})
	createIf(tbl.(TableChanger), t)
	start := time.Now().AddDate(0, 0, -1)
	tripTime := start
	for i := 0; i < 100; i++ {
		tripTime = tripTime.Add(7 * time.Minute)
		i := Trip{
			Id:   fmt.Sprintf("%v", i),
			Time: tripTime,
		}
		if err := tbl.Set(i).Run(); err != nil {
			t.Fatal(err)
		}
	}
	res := []Trip{}
	err := tbl.List(start.Add(2*time.Hour), time.Now().Add(-9*time.Hour), &res).WithOptions(Options{Limit: 13}).Run()
	if err != nil {
		t.Errorf("Failed to read 13 thingies: %s", err)
	}
	if len(res) != 13 {
		t.Errorf("Wrong number back: %d", len(res))
	}
	res1 := []Trip{}
	total := []Trip{}
	tbl1 := tbl.WithOptions(Options{Limit: 17})
	s, e := start.Add(2*time.Hour), start.Add(5*time.Hour)
	t.Logf("Start / End: %#v / %#v", s, e)
	n := 0
	for tbl1.List(s, e, &res1).Run() == nil && len(res1) > 1 {
		if n > 20 {
			break
		}
		n++
		t.Logf("Query a page of 17: %s", s.String())
		if len(res1) > 0 {
			total = append(total, res1[1:]...)
		} else {
			total = append(total, res1...)
		}
		s = res1[len(res1)-1].Time
	}
	if len(total) != 24 {
		t.Errorf("Pulled the wrong number of thingies: %d", len(total))
	}
	for i, to := range total {
		t.Logf("[%d] %#v", i, to)
	}
}
