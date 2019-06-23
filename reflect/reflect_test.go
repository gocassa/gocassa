package reflect

import (
	"reflect"

	"github.com/gocql/gocql"

	"testing"
)

type Tweet struct {
	Timeline      string
	ID            gocql.UUID `cql:"id"`
	Ignored       string     `cql:"-"`
	Text          string
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
		"ignored",
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
	if m["Text"] != tweet.Text {
		t.Errorf("Expected %s but got %s", tweet.Text, m["Text"])
	}
	if m["OriginalTweet"] != tweet.OriginalTweet {
		t.Errorf("Expected %v but got %s", tweet.OriginalTweet, m["OriginalTweet"])
	}
	if _, ok := m["Ignored"]; ok {
		t.Errorf("Igonred should be empty but got %s instead", m["Ignored"])
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
		//Ignored should be always empty
		if tweet.Ignored != "" {
			t.Errorf("Expected ignored to be empty but got %s",
				tweet.Ignored)
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
	m["Ignored"] = "ignored"
	assert()
}

func TestStructFieldMap(t *testing.T) {
	m, ok := StructFieldMap(Tweet{}, false)
	if !ok {
		t.Fatalf("expected field map to be created")
	}

	if timeline, ok := m["Timeline"]; ok {
		if timeline.Name() != "Timeline" {
			t.Errorf("Timeline should have name 'Timeline' but got %s", timeline.Name())
		}

		if timeline.Type() != reflect.TypeOf("") {
			t.Errorf("Timeline have type 'string' but got %s", timeline.Type())
		}
	} else {
		t.Errorf("Timeline should be present but wasn't: %+v", m)
	}

	if id, ok := m["id"]; ok {
		if id.Name() != "id" {
			t.Errorf("ID should have name 'id' but got %s", id.Name())
		}

		if id.Type() != reflect.TypeOf(gocql.UUID([16]byte{})) {
			t.Errorf("ID have type 'gocql.UUID' but got %s", id.Type())
		}
	} else {
		t.Errorf("ID should be present but wasn't: %+v", m)
	}

	if ot, ok := m["OriginalTweet"]; ok {
		if ot.Name() != "OriginalTweet" {
			t.Errorf("OriginalTweet should have name 'OriginalTweet' but got %s", ot.Name())
		}

		u := gocql.UUID([16]byte{})
		if ot.Type() != reflect.TypeOf(&u) {
			t.Errorf("OriginalTweet have type '*gocql.UUID' but got %s", ot.Type())
		}
	} else {
		t.Errorf("OriginalTweet should be present but wasn't: %+v", m)
	}

	if _, ok := m["Ignored"]; ok {
		t.Errorf("Ignored should not be present but got %v instead", m["Ignored"])
	}

	// Test lowercasing fields
	m2, ok := StructFieldMap(Tweet{}, true)
	if timeline, ok := m2["timeline"]; !ok {
		if timeline.Name() != "Timeline" {
			t.Errorf("Timeline should have name 'Timeline' but got %s", timeline.Name())
		}
	}
}

func TestStructFieldMapEmbeddedStruct(t *testing.T) {
	type EmbeddedTweet struct {
		*Tweet   `cql:",squash"`
		Embedder string
	}

	m, ok := StructFieldMap(EmbeddedTweet{}, false)
	if !ok {
		t.Fatalf("expected field map to be created")
	}

	if timeline, ok := m["Timeline"]; ok {
		if timeline.Name() != "Timeline" {
			t.Errorf("Timeline should have name 'Timeline' but got %s", timeline.Name())
		}

		if timeline.Type() != reflect.TypeOf("") {
			t.Errorf("Timeline have type 'string' but got %s", timeline.Type())
		}
	} else {
		t.Errorf("Timeline should be present but wasn't: %+v", m)
	}

	if embedder, ok := m["Embedder"]; ok {
		if embedder.Name() != "Embedder" {
			t.Errorf("Embedder should have name 'Embedder' but got %s", embedder.Name())
		}

		if embedder.Type() != reflect.TypeOf("") {
			t.Errorf("Embedder have type 'string' but got %s", embedder.Type())
		}
	} else {
		t.Errorf("Embedder should be present but wasn't: %+v", m)
	}

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
			[]string{"Timeline", "id", "Text", "OriginalTweet"},
			[]interface{}{"", emptyUUID, "", nilID},
		},
		{
			Tweet{"timeline1", id, "ignored", "hello gocassa", &id},
			[]string{"Timeline", "id", "Text", "OriginalTweet"},
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
