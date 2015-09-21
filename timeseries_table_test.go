package gocassa

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	require.NoError(t, err)
	ts := []Trip{}
	err = tbl.List(parse("2006 Jan 2 15:03:58"), parse("2006 Jan 2 15:04:02"), &ts).Run()
	require.NoError(t, err)
	assert.Len(t, ts, 3)
	assert.Equal(t, "1", ts[0].Id)
	assert.Equal(t, "2", ts[1].Id)
	assert.Equal(t, "3", ts[2].Id)
	err = tbl.List(parse("2006 Jan 2 15:03:58"), parse("2006 Jan 2 15:05:02"), &ts).Run()
	require.NoError(t, err)
	assert.Len(t, ts, 4)
}

func TestOptions(t *testing.T) {
	tbl := ns.TimeSeriesTable("tripTime6", "Time", "Id", 2*time.Hour, Trip{})
	createIf(tbl.(TableChanger), t)
	start := time.Now().Truncate(time.Second)
	end := start
	for i, tim := 0, start; i < 10; i++ {
		end = tim
		i := Trip{
			Id:   fmt.Sprintf("%v", i),
			Time: tim,
		}
		require.NoError(t, tbl.Set(i).Run())
		tim = tim.Add(time.Minute)
	}
	res := []Trip{}
	require.NoError(t, tbl.List(start, end, &res).Run())
	assert.Len(t, res, 10)
	require.NoError(t, tbl.WithOptions(Options{Limit: 3}).List(start, end, &res).Run())
	assert.Len(t, res, 3)
	res1 := []Trip{}
	require.NoError(t, tbl.List(start, end, &res1).WithOptions(Options{Limit: 2}).Run())
	assert.Len(t, res1, 2)
	listOp := tbl.List(start, end, &res)
	listOp1 := tbl.List(start, end, &res1)
	require.NoError(t, listOp.WithOptions(Options{Limit: 4}).Add(listOp1.WithOptions(Options{Limit: 5})).Run())
	if len(res) != 4 || len(res1) != 5 {
		t.Fatal(res, res1)
	}
	require.NoError(t, listOp.WithOptions(Options{Limit: 4}).Add(listOp1.WithOptions(Options{Limit: 5})).WithOptions(Options{Limit: 6}).Run())
	assert.Len(t, res, 6)
	assert.Len(t, res1, 6)
}
