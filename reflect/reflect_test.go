package reflect

import (
	"github.com/gocql/gocql"

	"testing"
)

type Tweet struct {
	Timeline      string
	ID            gocql.UUID  `cql:"id"`
	Text          string      `teXt`
	OriginalTweet *gocql.UUID `json:"origin"`
}

func TestStructToMap(t *testing.T) {
	//Test that if the value is not a struct we return nil, false
	m, ok := StructToMap("str")
	if m != nil {
		t.Error("map is not nil when val is a string")
	}
	if ok {
		t.Error("ok result from StructToMap when the val is a string")

	}

	tweet := Tweet{
		"t",
		gocql.TimeUUID(),
		"hello gocassa",
		nil,
	}

	m, ok = StructToMap(tweet)
	if !ok {
		t.Error("ok is false for a tweet")
	}

	if m["Timeline"] != tweet.Timeline {
		t.Errorf("Expected %s but got %s", tweet.Timeline, m["Timeline"])
	}

	if m["id"] != tweet.ID {
		t.Errorf("Expected %s but got %s", tweet.ID, m["id"])
	}
	if m["teXt"] != tweet.Text {
		t.Errorf("Expected %s but got %s", tweet.Text, m["teXt"])
	}
	if m["OriginalTweet"] != tweet.OriginalTweet {
		t.Errorf("Expected %v but got %s", tweet.OriginalTweet, m["OriginalTweet"])
	}

	id := gocql.TimeUUID()
	tweet.OriginalTweet = &id
	m, _ = StructToMap(tweet)
	if m["OriginalTweet"] != tweet.OriginalTweet {
		t.Errorf("Expected nil but got %s", m["OriginalTweet"])
	}
}

func TestMapToStruct(t *testing.T) {

	m := make(map[string]interface{})
	assert := func() {
		tweet := Tweet{}
		if err := MapToStruct(m, &tweet); err != nil {
			t.Fatal(err.Error())
		}
		timeline, ok := m["Timeline"]
		if ok {
			if timeline != tweet.Timeline {
				t.Errorf("Expected timeline to be %s but got %s", timeline, tweet.Timeline)
			}
		} else {
			if "" != tweet.Timeline {
				t.Errorf("Expected timeline to be empty but got %s", tweet.Timeline)
			}
		}
		id, ok := m["id"]
		if ok {
			if id != tweet.ID {
				t.Errorf("Expected id to be %s but got %s", id, tweet.ID)
			}
		} else {
			var emptyID gocql.UUID
			if emptyID != tweet.ID {
				t.Errorf("Expected id to be empty but got %s", tweet.ID)
			}
		}
		text, ok := m["teXt"]
		if ok {
			if text != tweet.Text {
				t.Errorf("Expected text to be %s but got %s", text, tweet.Text)
			}
		} else {
			if "" != tweet.Text {
				t.Errorf("Expected text to be empty but got %s", tweet.Text)
			}
		}

		originalTweet, ok := m["OriginalTweet"]
		if ok {
			if originalTweet != tweet.OriginalTweet {
				t.Errorf("Expected original tweet to be %s but got %s",
					originalTweet, tweet.OriginalTweet)
			}
		} else {
			if nil != tweet.OriginalTweet {
				t.Errorf("Expected original tweet to be empty but got %s",
					tweet.OriginalTweet)
			}
		}
	}

	assert()
	m["Timeline"] = "timeline"
	assert()
	m["id"] = gocql.TimeUUID()
	assert()
	m["text"] = "Hello gocassa"
	assert()
	id := gocql.TimeUUID()
	m["OriginalTweet"] = &id
	assert()
}

func TestFieldsAndValues(t *testing.T) {
	var emptyUUID gocql.UUID
	id := gocql.TimeUUID()
	var nilID *gocql.UUID
	var tests = []struct {
		tweet  Tweet
		fields []string
		values []interface{}
	}{
		{
			Tweet{},
			[]string{"Timeline", "id", "teXt", "OriginalTweet"},
			[]interface{}{"", emptyUUID, "", nilID},
		},
		{
			Tweet{"timeline1", id, "hello gocassa", &id},
			[]string{"Timeline", "id", "teXt", "OriginalTweet"},
			[]interface{}{"timeline1", id, "hello gocassa", &id},
		},
	}
	for _, test := range tests {
		fields, values, _ := FieldsAndValues(test.tweet)
		assertFieldsEqual(t, test.fields, fields)
		assertValuesEqual(t, test.values, values)
	}
}

func assertFieldsEqual(t *testing.T, a, b []string) {
	if len(a) != len(b) {
		t.Errorf("expected fields %v but got %v", a, b)
		return
	}

	for i := range a {
		if a[i] != b[i] {
			t.Errorf("expected fields %v but got %v", a, b)
		}
	}
}

func assertValuesEqual(t *testing.T, a, b []interface{}) {
	if len(a) != len(b) {
		t.Errorf("expected values %v but got %v different length", a, b)
		return
	}

	for i := range a {
		if a[i] != b[i] {
			t.Errorf("expected values %v but got %v a[i] = %v and b[i] = %v", a, b, a[i], b[i])
			return
		}
	}
}
