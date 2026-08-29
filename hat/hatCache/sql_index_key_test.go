package hatCache

import "testing"

func TestSQLIndexValueKeyUsesCompactTypedScalars(t *testing.T) {
	stringKey, stringOK := sqlIndexValueKey("true")
	boolKey, boolOK := sqlIndexValueKey(true)
	if !stringOK || !boolOK || stringKey != "s:true" || boolKey != "b:1" {
		t.Fatalf("typed scalar keys = %q/%t and %q/%t", stringKey, stringOK, boolKey, boolOK)
	}
	if stringKey == boolKey {
		t.Fatalf("string and boolean key collided: %q", stringKey)
	}
	numberKey, numberOK := sqlIndexValueKey(float64(7))
	if !numberOK || numberKey != "7" {
		t.Fatalf("number key = %q/%t, want 7/true", numberKey, numberOK)
	}
}
