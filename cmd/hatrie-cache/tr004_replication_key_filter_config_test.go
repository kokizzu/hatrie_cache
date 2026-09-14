package main

import (
	"io"
	"reflect"
	"testing"
)

func TestTR004ParseReplicationKeyPrefixes(t *testing.T) {
	cfg, err := parseConfig([]string{"-replication-key-prefixes", "region:eu:, region:asia:"}, io.Discard)
	if err != nil {
		t.Fatalf("parseConfig() error = %v", err)
	}
	if got := parseReplicationKeyPrefixes(cfg.replicationKeyPrefixes); !reflect.DeepEqual(got, []string{"region:eu:", "region:asia:"}) {
		t.Fatalf("parsed replication key prefixes = %#v", got)
	}
}

func TestTR004ParseReplicationKeyPrefixesDefaultDisabled(t *testing.T) {
	if got := parseReplicationKeyPrefixes(""); got != nil {
		t.Fatalf("empty replication key prefixes = %#v, want nil", got)
	}
}
