package hatCache

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"hatrie_cache/hat/hatSchema"
)

const (
	replicationMetaSchemaVersion     = "_hatrie_replication_schema_version"
	replicationMetaSchemaFingerprint = "_hatrie_replication_schema_fingerprint"
)

// ReplicationSchemaContract identifies the schema a replication writer used
// to produce a command stream. A receiver can require an exact match before
// applying the command.
type ReplicationSchemaContract struct {
	Version     uint64
	Fingerprint string
}

// ReplicationSchemaCompatibilityPolicy contains an explicit current schema
// contract and a private set of validated previous contracts accepted during a
// rolling deployment. Unknown contracts are never accepted.
type ReplicationSchemaCompatibilityPolicy struct {
	current  ReplicationSchemaContract
	accepted map[ReplicationSchemaContract]struct{}
}

// NewReplicationSchemaCompatibilityPolicy creates an explicit schema history
// for rolling replication. Every previous schema must be conservatively
// compatible with current; the supplied schemas are not retained or mutated.
func NewReplicationSchemaCompatibilityPolicy(current hatSchema.Schema, previous ...hatSchema.Schema) (*ReplicationSchemaCompatibilityPolicy, error) {
	if err := current.Validate(); err != nil {
		return nil, fmt.Errorf("current schema: %w", err)
	}
	currentContract := NewReplicationSchemaContract(current)
	if !currentContract.Configured() {
		return nil, errors.New("current schema has an incomplete replication contract")
	}
	policy := &ReplicationSchemaCompatibilityPolicy{
		current:  currentContract,
		accepted: make(map[ReplicationSchemaContract]struct{}, len(previous)+1),
	}
	policy.accepted[policy.current] = struct{}{}
	for index, schema := range previous {
		report, err := hatSchema.CheckRollingCompatibility(schema, current)
		if err != nil {
			return nil, fmt.Errorf("previous schema %d: %w", index, err)
		}
		if !report.Compatible {
			return nil, fmt.Errorf("previous schema %d is not rolling-compatible: %v", index, report.Changes)
		}
		contract := NewReplicationSchemaContract(schema)
		if !contract.Configured() {
			return nil, fmt.Errorf("previous schema %d has an incomplete replication contract", index)
		}
		policy.accepted[contract] = struct{}{}
	}
	return policy, nil
}

// Current returns the current contract represented by the policy.
func (policy *ReplicationSchemaCompatibilityPolicy) Current() ReplicationSchemaContract {
	if policy == nil {
		return ReplicationSchemaContract{}
	}
	return policy.current
}

// Accepts reports whether contract is the current contract or one of the
// explicitly registered validated previous contracts.
func (policy *ReplicationSchemaCompatibilityPolicy) Accepts(contract ReplicationSchemaContract) bool {
	if policy == nil || !contract.Configured() {
		return false
	}
	_, exists := policy.accepted[contract]
	return exists
}

// NewReplicationSchemaContract derives a replication contract from a schema.
func NewReplicationSchemaContract(schema hatSchema.Schema) ReplicationSchemaContract {
	return ReplicationSchemaContract{Version: schema.Version, Fingerprint: schema.Fingerprint()}
}

// Configured reports whether both contract components are present.
func (contract ReplicationSchemaContract) Configured() bool {
	return contract.Version > 0 && strings.TrimSpace(contract.Fingerprint) != ""
}

func replicationSchemaMetadata(request CacheCommandRequest) (ReplicationSchemaContract, bool, error) {
	versionValue, versionPresent := request.Pairs[replicationMetaSchemaVersion]
	fingerprintValue, fingerprintPresent := request.Pairs[replicationMetaSchemaFingerprint]
	if !versionPresent && !fingerprintPresent {
		return ReplicationSchemaContract{}, false, nil
	}
	if !versionPresent || !fingerprintPresent {
		return ReplicationSchemaContract{}, true, errors.New("schema contract requires version and fingerprint")
	}
	version, err := commandUint64Value(versionValue)
	if err != nil {
		return ReplicationSchemaContract{}, true, errors.New("schema contract version is invalid")
	}
	fingerprint, err := commandScalarString(fingerprintValue)
	if err != nil || strings.TrimSpace(fingerprint) == "" {
		return ReplicationSchemaContract{}, true, errors.New("schema contract fingerprint is invalid")
	}
	contract := ReplicationSchemaContract{Version: version, Fingerprint: strings.TrimSpace(fingerprint)}
	if !contract.Configured() {
		return ReplicationSchemaContract{}, true, errors.New("schema contract version is invalid")
	}
	return contract, true, nil
}

func replicationSchemaMetadataPairs(contract ReplicationSchemaContract) Map {
	if !contract.Configured() {
		return nil
	}
	return Map{
		replicationMetaSchemaVersion:     strconv.FormatUint(contract.Version, 10),
		replicationMetaSchemaFingerprint: strings.TrimSpace(contract.Fingerprint),
	}
}

func replicationSchemaMetadataWireValues(contract ReplicationSchemaContract) (string, string, bool) {
	if !contract.Configured() {
		return "", "", false
	}
	return strconv.FormatUint(contract.Version, 10), strings.TrimSpace(contract.Fingerprint), true
}

func (replicator *HTTPReplicator) currentReplicationSchema() ReplicationSchemaContract {
	if replicator == nil {
		return ReplicationSchemaContract{}
	}
	return replicator.replicationSchema
}
