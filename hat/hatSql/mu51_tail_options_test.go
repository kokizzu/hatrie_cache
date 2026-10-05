package hatSql

import "testing"

func TestMU51ParseTailOptionsIntoSubscriptionDefinition(t *testing.T) {
	statement, err := ParseSQLSubscriptionStatement(`TAIL FROM CACHE('items') SELECT id WITH (SNAPSHOT = false, PROGRESS = true, AS OF = 4, UP TO = 9, DETERMINISTIC = true)`)
	if err != nil {
		t.Fatal(err)
	}
	if statement.Mode != SQLSubscriptionModeDifferential {
		t.Fatalf("mode = %q, want differential", statement.Mode)
	}
	definition := statement.Definition
	if !definition.StartLive || !definition.EmitProgress || definition.AsOf != 4 || definition.UpTo != 9 || !definition.DeterministicOrder {
		t.Fatalf("TAIL definition = %#v, want parsed options", definition)
	}
}

func TestMU51TailOptionDefaultsAndLegacySyntax(t *testing.T) {
	legacy, err := ParseSQLSubscriptionStatement("TAIL FROM CACHE('items') SELECT id")
	if err != nil {
		t.Fatal(err)
	}
	if legacy.Mode != SQLSubscriptionModeDifferential || legacy.Definition.EmitProgress || legacy.Definition.StartLive || legacy.Definition.AsOf != 0 || legacy.Definition.UpTo != 0 {
		t.Fatalf("legacy TAIL definition = %#v, want unchanged defaults", legacy.Definition)
	}
	withDefaults, err := ParseSQLSubscriptionStatement(`TAIL FROM CACHE('items') SELECT id WITH (SNAPSHOT = true, PROGRESS = false)`)
	if err != nil {
		t.Fatal(err)
	}
	if withDefaults.Definition.StartLive || withDefaults.Definition.EmitProgress {
		t.Fatalf("explicit defaults = %#v, want snapshot and no progress", withDefaults.Definition)
	}
}

func TestMU51TailOptionsRejectInvalidOrConflictingValues(t *testing.T) {
	queries := []string{
		"TAIL FROM CACHE('items') SELECT id WITH (PROGRESS = maybe)",
		"TAIL FROM CACHE('items') SELECT id WITH (UNKNOWN = true)",
		"TAIL FROM CACHE('items') SELECT id WITH (AS OF = 9, UP TO = 4)",
		"TAIL FROM CACHE('items') SELECT id WITH (PROGRESS = true, PROGRESS = false)",
		"TAIL FROM CACHE('items') SELECT id WITH (PROGRESS)",
		"TAIL FROM CACHE('items') SELECT id WITH ()",
		"TAIL FROM CACHE('items') SELECT id WITH (PROGRESS = true))",
	}
	for _, query := range queries {
		if _, err := ParseSQLSubscriptionStatement(query); err == nil {
			t.Fatalf("ParseSQLSubscriptionStatement(%q) succeeded", query)
		}
	}
}

func TestMU51TailOptionsPreserveQueryText(t *testing.T) {
	statement, err := ParseSQLSubscriptionStatement(`TAIL FROM CACHE('items') SELECT id WITH (PROGRESS = true)`)
	if err != nil {
		t.Fatal(err)
	}
	if statement.Definition.Query != "FROM CACHE('items') SELECT id" {
		t.Fatalf("query = %q, want options removed", statement.Definition.Query)
	}
}

func TestMU51TailOptionsKeepCaseInsensitiveAndLegacyEdgeCasesSafe(t *testing.T) {
	statement, err := ParseSQLSubscriptionStatement(`tail FROM CACHE('items') SELECT id with (snapshot = FALSE, progress = TRUE)`)
	if err != nil {
		t.Fatal(err)
	}
	if !statement.Definition.StartLive || !statement.Definition.EmitProgress {
		t.Fatalf("case-insensitive options = %#v, want live progress", statement.Definition)
	}

	legacy, err := ParseSQLSubscriptionStatement("TAIL SELECT W)")
	if err != nil {
		t.Fatal(err)
	}
	if legacy.Definition.Query != "SELECT W)" {
		t.Fatalf("legacy edge query = %q, want preserved query", legacy.Definition.Query)
	}
}
