package gocassa

import (
	"testing"
	"time"

	"github.com/mattheath/base62"
	"github.com/mattheath/kala/bigflake"
	"github.com/mattheath/kala/util"
)

func timeToFlake(t *testing.T, time string) string {
	ts := parse(time)
	msTime := util.TimeToMsInt64(ts)
	bf := bigflake.MintId(msTime, 1000, 1000)
	return "id_" + base62.EncodeBigInt(bf)
}

func TestFlakeSeriesT(t *testing.T) {
	tbl := ns.FlakeSeriesTable("tripFlake5", "Id", time.Minute, Trip{})
	createIf(tbl.(TableChanger), t)
	id1 := timeToFlake(t, "2006 Jan 2 15:03:59")
	id2 := timeToFlake(t, "2006 Jan 2 15:04:00")
	id3 := timeToFlake(t, "2006 Jan 2 15:04:01")
	id4 := timeToFlake(t, "2006 Jan 2 15:05:01")

	err := tbl.Set(Trip{
		Id: id1,
	}).Add(tbl.Set(Trip{
		Id: id2,
	})).Add(tbl.Set(Trip{
		Id: id3,
	})).Add(tbl.Set(Trip{
		Id: id4,
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
	if ts1[0].Id != id1 || ts1[1].Id != id2 || ts1[2].Id != id3 {
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
