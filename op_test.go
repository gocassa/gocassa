package gocassa

import (
	"reflect"
	"testing"
)

type OpTestStruct struct {
	A string `cql:"id,omitempty"`
	B int
	C int `cql:"-"`
	D map[string]interface{}
	E []interface{}
	F []string
	G []OpTestStructAlias

	OpTestStructEmbed `cql:",squash"`
}

type OpTestStructEmbed struct {
	XA int
	XB OpTestStructAlias
	XC []string
}

type OpTestStructAlias string

var opTestRow = map[string]interface{}{
	"id": "A",
	"B":  1,
	"C":  1,
	"D": map[string]interface{}{
		"D1": 1,
		"D2": "2",
	},
	"E": []interface{}{
		"E1",
		"E2", "E3", 4,
	},
	"F": []string{
		"F1", "F2", "F3",
	},
	"G": []string{
		"F1", "F2", "F3",
	},

	"XA": 1,
	"XB": "XB",
	"XC": []string{
		"XC1", "XC2",
	},
}

var opTestRowExpected = OpTestStruct{
	A: "A",
	B: 1,
	C: 0,
	D: map[string]interface{}{"D1": 1, "D2": "2"},
	E: []interface{}{"E1", "E2", "E3", 4},
	F: []string{"F1", "F2", "F3"},
	G: []OpTestStructAlias{"F1", "F2", "F3"},
	OpTestStructEmbed: OpTestStructEmbed{
		XA: 1,
		XB: "XB",
		XC: []string{"XC1", "XC2"},
	},
}

func TestDecodeRow(t *testing.T) {
	var result []OpTestStruct

	err := decodeResult([]map[string]interface{}{opTestRow}, &result)
	if err != nil {
		t.Fatal(err)
	}

	if len(result) != 1 {
		t.Fatalf("Expected 1 results, got %d", len(result))
	}

	if !reflect.DeepEqual(result, []OpTestStruct{opTestRowExpected}) {
		t.Fatalf("Did not get expected result")
	}
}

func TestDecodeSingleRow(t *testing.T) {
	var result OpTestStruct

	err := decodeResult(opTestRow, &result)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(result, opTestRowExpected) {
		t.Fatalf("Did not get expected result")
	}
}
