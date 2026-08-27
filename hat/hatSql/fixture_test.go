package hatSql

import "testing"

func TestEmbeddedQueryFixturesAreParserValidAndNamed(t *testing.T) {
	fixtures, err := EmbeddedQueryFixtures()
	if err != nil {
		t.Fatalf("EmbeddedQueryFixtures() error = %v", err)
	}
	if len(fixtures) == 0 {
		t.Fatal("EmbeddedQueryFixtures() returned no fixtures")
	}
	fixture, ok := EmbeddedQueryFixture("basic-cache")
	if !ok {
		t.Fatal("EmbeddedQueryFixture(basic-cache) = false")
	}
	if fixture.Query == "" || len(fixture.Sources["users"]) != 2 || len(fixture.ExpectedRows) != 2 {
		t.Fatalf("basic-cache fixture = %#v, want reproducible rows and query", fixture)
	}
}
