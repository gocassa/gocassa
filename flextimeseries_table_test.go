package gocassa

import (
	"strings"
	"testing"
	"time"
)

type TripC struct {
	Id   string
	Time time.Time
	Tag  string
	Bag  string
	F2   string
}

type myTsBucketer struct {
	bucketSize time.Duration
}

func (b *myTsBucketer) Bucket(secs int64) int64 {
	return (secs - secs%int64(b.bucketSize/time.Second)) * 1000
}

func (b *myTsBucketer) Next(secs int64) int64 {
	return secs + int64(b.bucketSize/time.Second)*1000
}

func TestFlexTimeSeriesTable(t *testing.T) {
	tbl := ns.FlexMultiTimeSeriesTable("tripTime8", "Time", "Id", []string{"Tag"}, &tsBucketer{time.Minute}, TripB{})
	t.Logf("Table-name: %s bucketer name: %s", tbl.Name(), toString(&myTsBucketer{}))
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
	err = tbl.List(map[string]interface{}{"Tag": "A"}, parse("2006 Jan 2 15:03:58"), parse("2006 Jan 2 15:04:02"), ts).Run()
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
	err = tbl.List(map[string]interface{}{"Tag": "B"}, parse("2006 Jan 2 15:03:58"), parse("2006 Jan 2 15:04:02"), ts).Run()
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
	err = tbl.List(map[string]interface{}{"Tag": "B"}, parse("2006 Jan 2 15:03:58"), parse("2006 Jan 2 15:05:02"), ts).Run()
	if err != nil {
		t.Fatal(err)
	}
	if len(*ts) != 2 {
		t.Fatal(ts)
	}
	err = tbl.List(map[string]interface{}{"Tag": "B"}, parse("2006 Jan 2 15:03:58"), parse("2006 Jan 2 15:05:00"), ts).Run()
	if err != nil {
		t.Fatal(err)
	}
	if len(*ts) != 1 {
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

func TestFlexTimeSeriesTable2(t *testing.T) {
	tbl := ns.FlexMultiTimeSeriesTable("tripTime9", "Time", "Id", []string{"Tag", "Bag"}, &tsBucketer{time.Minute}, TripC{}).
		WithOptions(Options{TableName: "tt9_tag_bag"})
	t.Logf("Table-name: %s bucketer name: %s", tbl.Name(), toString(&myTsBucketer{}))
	createIf(tbl.(TableChanger), t)
	err := tbl.WithOptions(Options{TTL: 30 * time.Second}).Set(TripC{
		Id:   "1",
		Time: parse("2006 Jan 2 15:03:59"),
		Tag:  "A",
		Bag:  "III",
	}).Add(tbl.Set(TripC{
		Id:   "2",
		Time: parse("2006 Jan 2 15:04:00"),
		Tag:  "B",
		Bag:  "",
	})).Add(tbl.Set(TripC{
		Id:   "2.1",
		Time: parse("2006 Jan 2 15:04:01"),
		Tag:  "B",
		Bag:  "I",
	})).Add(tbl.Set(TripC{
		Id:   "2.2",
		Time: parse("2006 Jan 2 15:04:03"),
		Tag:  "B",
		Bag:  "I",
	})).Add(tbl.Set(TripC{
		Id:   "2.3",
		Time: parse("2006 Jan 2 15:04:05"),
		Tag:  "B",
		Bag:  "II",
	})).Add(tbl.Set(TripC{
		Id:   "3",
		Time: parse("2006 Jan 2 15:04:01"),
		Tag:  "A",
		Bag:  "III",
	})).Add(tbl.Set(TripC{
		Id:   "3.1",
		Time: parse("2006 Jan 2 15:04:01"),
		Tag:  "A",
		Bag:  "IV",
	})).Add(tbl.Set(TripC{
		Id:   "4",
		Time: parse("2006 Jan 2 15:05:01"),
		Tag:  "B",
		Bag:  "III",
	})).Run()
	if err != nil {
		t.Fatal(err)
	}
	ts := &[]TripC{}
	err = tbl.List(map[string]interface{}{"Tag": "A", "Bag": "III"}, parse("2006 Jan 2 15:03:58"), parse("2006 Jan 2 15:04:02"), ts).Run()
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
	err = tbl.List(map[string]interface{}{"Tag": "A", "Bag": "IV"}, parse("2006 Jan 2 15:03:58"), parse("2006 Jan 2 15:04:02"), ts).Run()
	if err != nil {
		t.Fatal(err)
	}
	if len(*ts) != 1 {
		t.Fatal(ts)
	}
	if ts1[0].Id != "1" {
		t.Fatal(ts1[0])
	}
	err = tbl.List(map[string]interface{}{"Tag": "B", "Bag": ""}, parse("2006 Jan 2 15:03:58"), parse("2006 Jan 2 15:04:02"), ts).Run()
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
	err = tbl.List(map[string]interface{}{"Tag": "B", "Bag": "I"}, parse("2006 Jan 2 15:03:58"), parse("2006 Jan 2 15:05:02"), ts).Run()
	if err != nil {
		t.Fatal(err)
	}
	if len(*ts) != 2 {
		t.Fatal(ts)
	}
	err = tbl.List(map[string]interface{}{"Tag": "B", "Bag": "III"}, parse("2006 Jan 2 15:03:58"), parse("2006 Jan 2 15:05:02"), ts).Run()
	if err != nil {
		t.Fatal(err)
	}
	if len(*ts) != 1 {
		t.Fatal(ts)
	}
	err = tbl.List(map[string]interface{}{"Tag": "B", "Bag": "III"}, parse("2006 Jan 2 15:03:58"), parse("2006 Jan 2 15:05:00"), ts).Run()
	if err != nil {
		t.Fatal(err)
	}
	if len(*ts) != 0 {
		t.Fatal(ts)
	}
	err = tbl.List("B", parse("2006 Jan 2 15:03:58"), parse("2006 Jan 2 15:05:00"), ts).Run()
	if err == nil || !strings.HasPrefix(err.Error(), "Must pass map of ") || !strings.HasSuffix(err.Error(), "B") {
		t.Fatal("Should have got an error")
	}
	err = tbl.List(map[string]interface{}{"Tag": "B"}, parse("2006 Jan 2 15:03:58"), parse("2006 Jan 2 15:05:00"), ts).Run()
	if err == nil || !strings.HasPrefix(err.Error(), "Indexes incomplete:") || !strings.HasSuffix(err.Error(), "[Tag Bag]") {
		t.Fatalf("Should have got an error: %s", err)
	}
}
