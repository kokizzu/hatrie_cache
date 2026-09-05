package hatBackup

// DoctorReport records offline verification of a backup artifact.
type DoctorReport struct {
	OK                  bool                 `json:"ok"`
	Kind                string               `json:"kind"`
	Path                string               `json:"path"`
	BackupID            string               `json:"backup_id,omitempty"`
	Snapshot            *DoctorSnapshot      `json:"snapshot,omitempty"`
	Journal             *DoctorJournal       `json:"journal,omitempty"`
	LevelDB             *DoctorStore         `json:"leveldb,omitempty"`
	Files               []BundleFile         `json:"files,omitempty"`
	Partition           *PartitionMetadata   `json:"partition,omitempty"`
	PartitionValidation *PartitionValidation `json:"partition_validation,omitempty"`
	RecoveredKeys       int                  `json:"recovered_keys,omitempty"`
	JournalSequence     uint64               `json:"journal_sequence,omitempty"`
	StateChecksum       string               `json:"state_checksum,omitempty"`
}

// DoctorSnapshot records verified snapshot contents.
type DoctorSnapshot struct {
	Path            string `json:"path"`
	OK              bool   `json:"ok"`
	Keys            int    `json:"keys"`
	JournalSequence uint64 `json:"journal_sequence"`
}

// DoctorJournal records verified journal contents.
type DoctorJournal struct {
	Path         string `json:"path"`
	OK           bool   `json:"ok"`
	Entries      int    `json:"entries"`
	LastSequence uint64 `json:"last_sequence"`
}

// DoctorStore records verified persistent-store contents.
type DoctorStore struct {
	Path    string `json:"path"`
	Backend string `json:"backend,omitempty"`
	OK      bool   `json:"ok"`
	Keys    int    `json:"keys"`
}

// PartitionValidation records whether a backup covers only declared keys.
type PartitionValidation struct {
	OK                       bool     `json:"ok"`
	CheckedKeys              int      `json:"checked_keys"`
	InvalidKeys              int      `json:"invalid_keys"`
	CheckedJournalKeys       int      `json:"checked_journal_keys,omitempty"`
	InvalidJournalKeys       int      `json:"invalid_journal_keys,omitempty"`
	KeyPrefixes              []string `json:"key_prefixes,omitempty"`
	InvalidKeySamples        []string `json:"invalid_key_samples,omitempty"`
	InvalidJournalKeySamples []string `json:"invalid_journal_key_samples,omitempty"`
}

// RestoreOptions controls restore publication behavior.
type RestoreOptions struct {
	Overwrite bool
}

// RestoreReport describes an applied offline restore.
type RestoreReport struct {
	OK                  bool                 `json:"ok"`
	Bundle              string               `json:"bundle"`
	DataDir             string               `json:"data_dir"`
	BackupID            string               `json:"backup_id,omitempty"`
	Mode                Mode                 `json:"mode,omitempty"`
	Snapshot            string               `json:"snapshot"`
	Store               string               `json:"store,omitempty"`
	StorageBackend      string               `json:"storage_backend,omitempty"`
	Journal             string               `json:"journal,omitempty"`
	Partition           *PartitionMetadata   `json:"partition,omitempty"`
	PartitionValidation *PartitionValidation `json:"partition_validation,omitempty"`
	JournalSequence     uint64               `json:"journal_sequence"`
	RecoveredKeys       int                  `json:"recovered_keys"`
}

// RehearsalOptions configures a non-production restore rehearsal.
type RehearsalOptions struct {
	WorkDir     string
	KeepWorkDir bool
}

// RehearsalGetCheck records a single post-restore key probe.
type RehearsalGetCheck struct {
	Key      string  `json:"key"`
	OK       bool    `json:"ok"`
	Value    string  `json:"value,omitempty"`
	Expected *string `json:"expected,omitempty"`
	Error    string  `json:"error,omitempty"`
}
