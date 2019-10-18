package gocassa

import (
	"testing"
	"time"
)

func TestAnyEquals(t *testing.T) {
	loc1, _ := time.LoadLocation("Europe/London")
	loc2, _ := time.LoadLocation("Europe/Berlin")
	testTime1, _ := time.ParseInLocation(time.RFC3339, "2013-08-05T09:00:00+01:00", loc1)
	testTime2, _ := time.ParseInLocation(time.RFC3339, "2013-08-05T08:00:00Z", loc2)

	type name string

	testCases := []struct {
		term      interface{}
		relations []interface{}
	}{
		{time.Minute * 2, makeInterfaceArray(time.Second * 120)},
		{testTime1, makeInterfaceArray(testTime2)},
		{1950, makeInterfaceArray(1950)},
		{[]byte{0x00, 0xFF, 0x01, 0x99, 0xEA}, makeInterfaceArray("\x00\xFF\x01\x99\xEA")},
		{name("Bingo 🐕"), makeInterfaceArray("Bingo 🐕")},
	}

	for _, tc := range testCases {
		equality := anyEquals(tc.term, tc.relations)
		if !equality {
			t.Fatalf("not equal (testcase: %v)", tc)
		}
	}
}

func makeInterfaceArray(terms ...interface{}) []interface{} {
	interfaceSlice := make([]interface{}, len(terms))
	for i, d := range terms {
		interfaceSlice[i] = d
	}
	return interfaceSlice
}
