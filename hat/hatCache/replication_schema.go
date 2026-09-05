package hatCache

import (
	"errors"
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
