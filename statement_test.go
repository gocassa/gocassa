package gocassa

import (
	"testing"
)

func TestStatement(t *testing.T) {
	query1 := "DROP KEYSPACE IF EXISTS fakekeyspace"
	stmtPlain := newStatement(query1, []interface{}{})
	if stmtPlain.Query() != query1 {
		t.Fatalf("expected query '%s', got '%s'", query1, stmtPlain.Query())
	}
	if len(stmtPlain.FieldNames()) != 0 {
		t.Fatalf("expected 0 fields, got %d fields", len(stmtPlain.FieldNames()))
	}
	if len(stmtPlain.Values()) != 0 {
		t.Fatalf("expected 0 values, got %d values", len(stmtPlain.FieldNames()))
	}

	query2 := "DELETE FROM fakekeyspace.faketable WHERE id = ?"
	stmtWithValues := newStatement(query2, []interface{}{"id_abcd1234"})
	if stmtWithValues.Query() != query2 {
		t.Fatalf("expected query '%s', got '%s'", query2, stmtWithValues.Query())
	}
	if len(stmtWithValues.FieldNames()) != 0 {
		t.Fatalf("expected 0 fields, got %d fields", len(stmtWithValues.FieldNames()))
	}
	if len(stmtWithValues.Values()) != 1 {
		t.Fatalf("expected 1 value, got %d values", len(stmtWithValues.FieldNames()))
	}

	query3 := "SELECT id, name, created FROM fakekeyspace.faketable WHERE id = ?"
	stmtSelect := newSelectStatement(query3, []interface{}{"id_abcd1234"}, []string{"id", "name", "created"})
	if stmtSelect.Query() != query3 {
		t.Fatalf("expected query '%s', got '%s'", query3, stmtSelect.Query())
	}
	if len(stmtSelect.FieldNames()) != 3 {
		t.Fatalf("expected 3 fields, got %d fields", len(stmtSelect.FieldNames()))
	}
	if len(stmtSelect.Values()) != 1 {
		t.Fatalf("expected 1 value, got %d values", len(stmtSelect.FieldNames()))
	}
}
