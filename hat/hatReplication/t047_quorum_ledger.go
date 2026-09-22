package hatReplication

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

const (
	// DefaultClusterWriteCommitLedgerMaxEntries bounds the number of unresolved
	// and completed transaction records retained by a default ledger.
	DefaultClusterWriteCommitLedgerMaxEntries = 1024
	// MaxClusterWriteCommitLedgerEntries prevents an accidental unbounded ledger.
	MaxClusterWriteCommitLedgerEntries = 100_000
	// DefaultClusterWriteCommitLedgerMaxFileBytes bounds the default ledger file.
	DefaultClusterWriteCommitLedgerMaxFileBytes = 16 << 20
	// MaxClusterWriteCommitLedgerFileBytes prevents oversized recovery input.
	MaxClusterWriteCommitLedgerFileBytes = 64 << 20

	clusterWriteCommitLedgerVersion  = 1
	clusterWriteCommitLedgerHeader   = 12
	clusterWriteCommitLedgerMagic    = "HQL1"
	clusterWriteCommitLedgerFileMode = 0o600
)

var (
	// ErrClusterWriteCommitLedgerInvalid indicates invalid ledger options or a
	// transaction identity that cannot be recorded safely.
	ErrClusterWriteCommitLedgerInvalid = errors.New("hatReplication: cluster write commit ledger input is invalid")
	// ErrClusterWriteCommitLedgerCorrupt indicates an invalid or truncated ledger
	// file. The caller must restore a known-good copy before retrying.
	ErrClusterWriteCommitLedgerCorrupt = errors.New("hatReplication: cluster write commit ledger is corrupt")
	// ErrClusterWriteCommitLedgerClosed indicates use after Close.
	ErrClusterWriteCommitLedgerClosed = errors.New("hatReplication: cluster write commit ledger is closed")
	// ErrClusterWriteCommitLedgerConflict indicates reuse of a transaction ID
	// with a different proposal or participant list.
	ErrClusterWriteCommitLedgerConflict = errors.New("hatReplication: cluster write commit ledger transaction conflicts")
	// ErrClusterWriteCommitLedgerReconcileRequired prevents blind retries after a
	// durable operation has an incomplete or unknown outcome.
	ErrClusterWriteCommitLedgerReconcileRequired = errors.New("hatReplication: cluster write commit ledger reconciliation is required")
	// ErrClusterWriteCommitLedgerFull indicates that the configured bounded
	// ledger has no room for a new transaction record.
	ErrClusterWriteCommitLedgerFull = errors.New("hatReplication: cluster write commit ledger is full")
	// ErrClusterWriteCommitLedgerNotFound indicates that Forget was given an
	// unknown transaction ID.
	ErrClusterWriteCommitLedgerNotFound = errors.New("hatReplication: cluster write commit ledger transaction was not found")
)

// ClusterWriteCommitLedgerPhase is the durable phase of one transaction.
type ClusterWriteCommitLedgerPhase string

const (
	ClusterWriteCommitLedgerPreparing      ClusterWriteCommitLedgerPhase = "preparing"
	ClusterWriteCommitLedgerPrepared       ClusterWriteCommitLedgerPhase = "prepared"
	ClusterWriteCommitLedgerCommitting     ClusterWriteCommitLedgerPhase = "committing"
	ClusterWriteCommitLedgerCommitted      ClusterWriteCommitLedgerPhase = "committed"
	ClusterWriteCommitLedgerAborted        ClusterWriteCommitLedgerPhase = "aborted"
	ClusterWriteCommitLedgerOutcomeUnknown ClusterWriteCommitLedgerPhase = "outcome_unknown"
)

// ClusterWriteCommitLedgerOptions bounds durable participant state. Zero uses
// the documented defaults. The ledger is opt-in; existing coordinators do not
// create a file unless this type is opened and passed to the ledger-aware API.
type ClusterWriteCommitLedgerOptions struct {
	MaxEntries   int
	MaxFileBytes int
}

// ClusterWriteCommitLedgerParticipant records one participant's durable phase
// acknowledgements in the original participant order.
type ClusterWriteCommitLedgerParticipant struct {
	Node      string `json:"node"`
	Prepared  bool   `json:"prepared,omitempty"`
	Aborted   bool   `json:"aborted,omitempty"`
	Committed bool   `json:"committed,omitempty"`
}

// ClusterWriteCommitLedgerRecord is a durable replay and reconciliation record.
type ClusterWriteCommitLedgerRecord struct {
	Proposal     ClusterWriteCommitProposal            `json:"proposal"`
	Participants []ClusterWriteCommitLedgerParticipant `json:"participants"`
	Phase        ClusterWriteCommitLedgerPhase         `json:"phase"`
}

// ClusterWriteCommitLedger stores bounded transaction outcomes in a small
// atomically replaced file. Every mutating operation is serialized so a caller
// can safely share one ledger between concurrent coordinator calls.
type ClusterWriteCommitLedger struct {
	mu           sync.Mutex
	path         string
	maxEntries   int
	maxFileBytes int
	records      map[string]ClusterWriteCommitLedgerRecord
	closed       bool
}

type clusterWriteCommitLedgerDisk struct {
	Version uint32                           `json:"version"`
	Records []ClusterWriteCommitLedgerRecord `json:"records"`
}

// OpenClusterWriteCommitLedger opens or creates a bounded durable quorum
// ledger. The parent directory must already exist and neither the ledger path
// nor its parent may be a symlink.
func OpenClusterWriteCommitLedger(path string, options ClusterWriteCommitLedgerOptions) (*ClusterWriteCommitLedger, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, ErrClusterWriteCommitLedgerInvalid
	}
	cleanPath := filepath.Clean(path)
	if cleanPath == "." || filepath.Base(cleanPath) == "." || filepath.Base(cleanPath) == string(filepath.Separator) {
		return nil, ErrClusterWriteCommitLedgerInvalid
	}
	normalizedOptions, err := normalizeClusterWriteCommitLedgerOptions(options)
	if err != nil {
		return nil, err
	}
	parent := filepath.Dir(cleanPath)
	parentInfo, err := os.Lstat(parent)
	if err != nil {
		return nil, fmt.Errorf("%w: ledger parent: %v", ErrClusterWriteCommitLedgerInvalid, err)
	}
	if parentInfo.Mode()&os.ModeSymlink != 0 || !parentInfo.IsDir() {
		return nil, fmt.Errorf("%w: ledger parent is not a real directory", ErrClusterWriteCommitLedgerInvalid)
	}

	ledger := &ClusterWriteCommitLedger{
		path:         cleanPath,
		maxEntries:   normalizedOptions.MaxEntries,
		maxFileBytes: normalizedOptions.MaxFileBytes,
		records:      make(map[string]ClusterWriteCommitLedgerRecord),
	}
	info, err := os.Lstat(cleanPath)
	if errors.Is(err, os.ErrNotExist) {
		if err := ledger.persistLocked(); err != nil {
			return nil, err
		}
		return ledger, nil
	}
	if err != nil {
		return nil, fmt.Errorf("%w: ledger stat: %v", ErrClusterWriteCommitLedgerCorrupt, err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return nil, fmt.Errorf("%w: ledger path is not a regular file", ErrClusterWriteCommitLedgerCorrupt)
	}
	if info.Size() < clusterWriteCommitLedgerHeader || info.Size() > int64(normalizedOptions.MaxFileBytes) {
		return nil, fmt.Errorf("%w: ledger size %d is outside bounds", ErrClusterWriteCommitLedgerCorrupt, info.Size())
	}
	data, err := os.ReadFile(cleanPath)
	if err != nil {
		return nil, fmt.Errorf("%w: read ledger: %v", ErrClusterWriteCommitLedgerCorrupt, err)
	}
	records, err := decodeClusterWriteCommitLedger(data, normalizedOptions.MaxEntries, normalizedOptions.MaxFileBytes)
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(cleanPath, clusterWriteCommitLedgerFileMode); err != nil {
		return nil, fmt.Errorf("%w: secure ledger permissions: %v", ErrClusterWriteCommitLedgerInvalid, err)
	}
	ledger.records = records
	return ledger, nil
}

// Close prevents further ledger mutations. It is idempotent; each successful
// transition is already persisted before its caller observes success.
func (ledger *ClusterWriteCommitLedger) Close() error {
	if ledger == nil {
		return nil
	}
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if ledger.closed {
		return nil
	}
	ledger.closed = true
	return nil
}

// Record returns an immutable copy of a transaction record.
func (ledger *ClusterWriteCommitLedger) Record(transactionID string) (ClusterWriteCommitLedgerRecord, bool) {
	if ledger == nil {
		return ClusterWriteCommitLedgerRecord{}, false
	}
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if ledger.closed {
		return ClusterWriteCommitLedgerRecord{}, false
	}
	record, ok := ledger.records[strings.TrimSpace(transactionID)]
	if !ok {
		return ClusterWriteCommitLedgerRecord{}, false
	}
	return cloneClusterWriteCommitLedgerRecord(record), true
}

// Forget removes a committed or aborted transaction after the caller no longer
// needs replay protection for it. Incomplete outcomes cannot be forgotten
// because doing so would make a blind retry possible.
func (ledger *ClusterWriteCommitLedger) Forget(transactionID string) error {
	if ledger == nil {
		return ErrClusterWriteCommitLedgerInvalid
	}
	transactionID = strings.TrimSpace(transactionID)
	if transactionID == "" {
		return ErrClusterWriteCommitLedgerInvalid
	}
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if ledger.closed {
		return ErrClusterWriteCommitLedgerClosed
	}
	record, ok := ledger.records[transactionID]
	if !ok {
		return ErrClusterWriteCommitLedgerNotFound
	}
	if record.Phase != ClusterWriteCommitLedgerCommitted && record.Phase != ClusterWriteCommitLedgerAborted {
		return ErrClusterWriteCommitLedgerReconcileRequired
	}
	delete(ledger.records, transactionID)
	if err := ledger.persistLocked(); err != nil {
		ledger.records[transactionID] = record
		return err
	}
	return nil
}

// ExecuteClusterWriteCommitWithLedger adds durable replay protection and
// participant outcome state to ExecuteClusterWriteCommit. A nil ledger keeps
// the original coordinator behavior and does not allocate ledger state.
func ExecuteClusterWriteCommitWithLedger(
	ctx context.Context,
	nodes []string,
	proposal ClusterWriteCommitProposal,
	prepare ClusterWriteCommitPrepareFunc,
	commit ClusterWriteCommitCommitFunc,
	abort ClusterWriteCommitAbortFunc,
	ledger *ClusterWriteCommitLedger,
) (ClusterWriteCommitResult, error) {
	if ledger == nil {
		return ExecuteClusterWriteCommit(ctx, nodes, proposal, prepare, commit, abort)
	}
	proposal.TransactionID = strings.TrimSpace(proposal.TransactionID)
	result := ClusterWriteCommitResult{Proposal: proposal}
	if ctx == nil || proposal.TransactionID == "" || prepare == nil || commit == nil || abort == nil {
		return result, ErrClusterWriteCommitInvalid
	}
	normalizedNodes, err := normalizeClusterWriteCommitNodes(nodes)
	if err != nil {
		return result, err
	}
	if err := ctx.Err(); err != nil {
		return result, errors.Join(ErrClusterWriteCommitPrepareFailed, err)
	}
	record, alreadyCommitted, err := ledger.begin(proposal, normalizedNodes)
	if err != nil {
		return result, err
	}
	if alreadyCommitted {
		return clusterWriteCommitResultFromLedger(record), nil
	}

	prepareWithLedger := func(callbackContext context.Context, node string, callbackProposal ClusterWriteCommitProposal) error {
		if err := prepare(callbackContext, node, callbackProposal); err != nil {
			return err
		}
		allPrepared, err := ledger.markPrepared(callbackProposal.TransactionID, node)
		if err != nil {
			return err
		}
		if allPrepared {
			if err := ledger.markCommitting(callbackProposal.TransactionID); err != nil {
				return err
			}
		}
		return nil
	}
	commitWithLedger := func(callbackContext context.Context, node string, callbackProposal ClusterWriteCommitProposal) error {
		if err := commit(callbackContext, node, callbackProposal); err != nil {
			return err
		}
		return ledger.markCommitted(callbackProposal.TransactionID, node)
	}
	abortWithLedger := func(callbackContext context.Context, node string, callbackProposal ClusterWriteCommitProposal) error {
		if err := abort(callbackContext, node, callbackProposal); err != nil {
			return err
		}
		return ledger.markAborted(callbackProposal.TransactionID, node)
	}

	result, err = ExecuteClusterWriteCommit(ctx, normalizedNodes, proposal, prepareWithLedger, commitWithLedger, abortWithLedger)
	if result.OutcomeUnknown {
		if ledgerErr := ledger.markOutcomeUnknown(proposal.TransactionID); ledgerErr != nil {
			return result, errors.Join(err, ledgerErr)
		}
		return result, err
	}
	return result, err
}

func normalizeClusterWriteCommitLedgerOptions(options ClusterWriteCommitLedgerOptions) (ClusterWriteCommitLedgerOptions, error) {
	if options.MaxEntries == 0 {
		options.MaxEntries = DefaultClusterWriteCommitLedgerMaxEntries
	}
	if options.MaxFileBytes == 0 {
		options.MaxFileBytes = DefaultClusterWriteCommitLedgerMaxFileBytes
	}
	if options.MaxEntries < 1 || options.MaxEntries > MaxClusterWriteCommitLedgerEntries {
		return ClusterWriteCommitLedgerOptions{}, fmt.Errorf("%w: max entries %d", ErrClusterWriteCommitLedgerInvalid, options.MaxEntries)
	}
	if options.MaxFileBytes < clusterWriteCommitLedgerHeader || options.MaxFileBytes > MaxClusterWriteCommitLedgerFileBytes {
		return ClusterWriteCommitLedgerOptions{}, fmt.Errorf("%w: max file bytes %d", ErrClusterWriteCommitLedgerInvalid, options.MaxFileBytes)
	}
	return options, nil
}

func (ledger *ClusterWriteCommitLedger) begin(proposal ClusterWriteCommitProposal, nodes []string) (ClusterWriteCommitLedgerRecord, bool, error) {
	proposal.TransactionID = strings.TrimSpace(proposal.TransactionID)
	if proposal.TransactionID == "" {
		return ClusterWriteCommitLedgerRecord{}, false, ErrClusterWriteCommitLedgerInvalid
	}
	normalizedNodes, err := normalizeClusterWriteCommitNodes(nodes)
	if err != nil {
		return ClusterWriteCommitLedgerRecord{}, false, err
	}
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if ledger.closed {
		return ClusterWriteCommitLedgerRecord{}, false, ErrClusterWriteCommitLedgerClosed
	}
	if existing, ok := ledger.records[proposal.TransactionID]; ok {
		if existing.Proposal != proposal || !clusterWriteCommitLedgerParticipantsMatch(existing.Participants, normalizedNodes) {
			return ClusterWriteCommitLedgerRecord{}, false, ErrClusterWriteCommitLedgerConflict
		}
		if existing.Phase == ClusterWriteCommitLedgerCommitted {
			return cloneClusterWriteCommitLedgerRecord(existing), true, nil
		}
		return ClusterWriteCommitLedgerRecord{}, false, ErrClusterWriteCommitLedgerReconcileRequired
	}
	if len(ledger.records) >= ledger.maxEntries {
		return ClusterWriteCommitLedgerRecord{}, false, ErrClusterWriteCommitLedgerFull
	}
	record := ClusterWriteCommitLedgerRecord{
		Proposal:     proposal,
		Phase:        ClusterWriteCommitLedgerPreparing,
		Participants: make([]ClusterWriteCommitLedgerParticipant, len(normalizedNodes)),
	}
	for index, node := range normalizedNodes {
		record.Participants[index].Node = node
	}
	ledger.records[proposal.TransactionID] = record
	if err := ledger.persistLocked(); err != nil {
		delete(ledger.records, proposal.TransactionID)
		return ClusterWriteCommitLedgerRecord{}, false, err
	}
	return cloneClusterWriteCommitLedgerRecord(record), false, nil
}

func (ledger *ClusterWriteCommitLedger) markPrepared(transactionID, node string) (bool, error) {
	allPrepared := false
	err := ledger.updateRecord(transactionID, func(record *ClusterWriteCommitLedgerRecord) error {
		participant, ok := clusterWriteCommitLedgerParticipant(record, node)
		if !ok || participant.Aborted || participant.Committed {
			return ErrClusterWriteCommitLedgerReconcileRequired
		}
		participant.Prepared = true
		allPrepared = true
		for _, value := range record.Participants {
			if !value.Prepared {
				allPrepared = false
				break
			}
		}
		if allPrepared && record.Phase == ClusterWriteCommitLedgerPreparing {
			record.Phase = ClusterWriteCommitLedgerPrepared
		}
		return nil
	}, func(record ClusterWriteCommitLedgerRecord) error {
		if !allPrepared {
			return nil
		}
		return ledger.markCommittingLocked(record.Proposal.TransactionID)
	})
	return allPrepared, err
}

func (ledger *ClusterWriteCommitLedger) markCommitting(transactionID string) error {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	return ledger.markCommittingLocked(transactionID)
}

func (ledger *ClusterWriteCommitLedger) markCommittingLocked(transactionID string) error {
	if ledger.closed {
		return ErrClusterWriteCommitLedgerClosed
	}
	record, ok := ledger.records[strings.TrimSpace(transactionID)]
	if !ok {
		return ErrClusterWriteCommitLedgerCorrupt
	}
	if record.Phase == ClusterWriteCommitLedgerCommitting || record.Phase == ClusterWriteCommitLedgerCommitted {
		return nil
	}
	if record.Phase != ClusterWriteCommitLedgerPrepared {
		return ErrClusterWriteCommitLedgerReconcileRequired
	}
	old := cloneClusterWriteCommitLedgerRecord(record)
	record.Phase = ClusterWriteCommitLedgerCommitting
	ledger.records[transactionID] = record
	if err := ledger.persistLocked(); err != nil {
		ledger.records[transactionID] = old
		return err
	}
	return nil
}

func (ledger *ClusterWriteCommitLedger) markCommitted(transactionID, node string) error {
	return ledger.updateRecord(transactionID, func(record *ClusterWriteCommitLedgerRecord) error {
		participant, ok := clusterWriteCommitLedgerParticipant(record, node)
		if !ok || participant.Aborted || !participant.Prepared {
			return ErrClusterWriteCommitLedgerReconcileRequired
		}
		participant.Committed = true
		allCommitted := true
		for _, value := range record.Participants {
			if !value.Committed {
				allCommitted = false
				break
			}
		}
		if allCommitted {
			record.Phase = ClusterWriteCommitLedgerCommitted
		}
		return nil
	}, nil)
}

func (ledger *ClusterWriteCommitLedger) markAborted(transactionID, node string) error {
	return ledger.updateRecord(transactionID, func(record *ClusterWriteCommitLedgerRecord) error {
		participant, ok := clusterWriteCommitLedgerParticipant(record, node)
		if !ok || participant.Committed {
			return ErrClusterWriteCommitLedgerReconcileRequired
		}
		participant.Aborted = true
		allAborted := true
		for _, value := range record.Participants {
			if value.Prepared && !value.Aborted {
				allAborted = false
				break
			}
		}
		if allAborted {
			record.Phase = ClusterWriteCommitLedgerAborted
		}
		return nil
	}, nil)
}

func (ledger *ClusterWriteCommitLedger) markOutcomeUnknown(transactionID string) error {
	return ledger.updateRecord(transactionID, func(record *ClusterWriteCommitLedgerRecord) error {
		if record.Phase == ClusterWriteCommitLedgerCommitted {
			return nil
		}
		record.Phase = ClusterWriteCommitLedgerOutcomeUnknown
		return nil
	}, nil)
}

func (ledger *ClusterWriteCommitLedger) updateRecord(transactionID string, update func(*ClusterWriteCommitLedgerRecord) error, after func(ClusterWriteCommitLedgerRecord) error) error {
	if ledger == nil {
		return ErrClusterWriteCommitLedgerInvalid
	}
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if ledger.closed {
		return ErrClusterWriteCommitLedgerClosed
	}
	transactionID = strings.TrimSpace(transactionID)
	record, ok := ledger.records[transactionID]
	if !ok {
		return ErrClusterWriteCommitLedgerCorrupt
	}
	old := cloneClusterWriteCommitLedgerRecord(record)
	if err := update(&record); err != nil {
		return err
	}
	ledger.records[transactionID] = record
	if err := ledger.persistLocked(); err != nil {
		ledger.records[transactionID] = old
		return err
	}
	if after != nil {
		if err := after(record); err != nil {
			return err
		}
	}
	return nil
}

func (ledger *ClusterWriteCommitLedger) persistLocked() error {
	disk := clusterWriteCommitLedgerDisk{
		Version: clusterWriteCommitLedgerVersion,
		Records: make([]ClusterWriteCommitLedgerRecord, 0, len(ledger.records)),
	}
	keys := make([]string, 0, len(ledger.records))
	for transactionID := range ledger.records {
		keys = append(keys, transactionID)
	}
	sort.Strings(keys)
	for _, transactionID := range keys {
		disk.Records = append(disk.Records, cloneClusterWriteCommitLedgerRecord(ledger.records[transactionID]))
	}
	payload, err := json.Marshal(disk)
	if err != nil {
		return fmt.Errorf("%w: encode ledger: %v", ErrClusterWriteCommitLedgerCorrupt, err)
	}
	if len(payload)+clusterWriteCommitLedgerHeader > ledger.maxFileBytes {
		return ErrClusterWriteCommitLedgerFull
	}
	data := make([]byte, clusterWriteCommitLedgerHeader+len(payload))
	copy(data, clusterWriteCommitLedgerMagic)
	binary.LittleEndian.PutUint32(data[4:8], uint32(len(payload)))
	binary.LittleEndian.PutUint32(data[8:12], crc32.ChecksumIEEE(payload))
	copy(data[clusterWriteCommitLedgerHeader:], payload)

	temporary, err := os.CreateTemp(filepath.Dir(ledger.path), "."+filepath.Base(ledger.path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("persist ledger temp file: %w", err)
	}
	temporaryPath := temporary.Name()
	removeTemporary := true
	defer func() {
		if removeTemporary {
			_ = os.Remove(temporaryPath)
		}
	}()
	if err := temporary.Chmod(clusterWriteCommitLedgerFileMode); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("persist ledger permissions: %w", err)
	}
	if written, err := temporary.Write(data); err != nil || written != len(data) {
		_ = temporary.Close()
		if err == nil {
			err = fmt.Errorf("short write: %d/%d bytes", written, len(data))
		}
		return fmt.Errorf("persist ledger data: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("persist ledger data sync: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("persist ledger data close: %w", err)
	}
	if err := os.Rename(temporaryPath, ledger.path); err != nil {
		return fmt.Errorf("replace ledger: %w", err)
	}
	removeTemporary = false
	parent, err := os.Open(filepath.Dir(ledger.path))
	if err != nil {
		return fmt.Errorf("open ledger parent for sync: %w", err)
	}
	syncErr := parent.Sync()
	closeErr := parent.Close()
	if syncErr != nil {
		return fmt.Errorf("sync ledger parent: %w", syncErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close ledger parent: %w", closeErr)
	}
	return nil
}

func decodeClusterWriteCommitLedger(data []byte, maxEntries, maxFileBytes int) (map[string]ClusterWriteCommitLedgerRecord, error) {
	if len(data) < clusterWriteCommitLedgerHeader || len(data) > maxFileBytes || string(data[:4]) != clusterWriteCommitLedgerMagic {
		return nil, ErrClusterWriteCommitLedgerCorrupt
	}
	payloadLength := int(binary.LittleEndian.Uint32(data[4:8]))
	if payloadLength < 0 || payloadLength != len(data)-clusterWriteCommitLedgerHeader {
		return nil, ErrClusterWriteCommitLedgerCorrupt
	}
	payload := data[clusterWriteCommitLedgerHeader:]
	if crc32.ChecksumIEEE(payload) != binary.LittleEndian.Uint32(data[8:12]) {
		return nil, ErrClusterWriteCommitLedgerCorrupt
	}
	var disk clusterWriteCommitLedgerDisk
	if err := json.Unmarshal(payload, &disk); err != nil {
		return nil, fmt.Errorf("%w: decode ledger: %v", ErrClusterWriteCommitLedgerCorrupt, err)
	}
	if disk.Version != clusterWriteCommitLedgerVersion || len(disk.Records) > maxEntries {
		return nil, ErrClusterWriteCommitLedgerCorrupt
	}
	records := make(map[string]ClusterWriteCommitLedgerRecord, len(disk.Records))
	for _, record := range disk.Records {
		if err := validateClusterWriteCommitLedgerRecord(record); err != nil {
			return nil, err
		}
		transactionID := record.Proposal.TransactionID
		if _, exists := records[transactionID]; exists {
			return nil, ErrClusterWriteCommitLedgerCorrupt
		}
		records[transactionID] = cloneClusterWriteCommitLedgerRecord(record)
	}
	return records, nil
}

func validateClusterWriteCommitLedgerRecord(record ClusterWriteCommitLedgerRecord) error {
	if record.Proposal.TransactionID == "" || strings.TrimSpace(record.Proposal.TransactionID) != record.Proposal.TransactionID || !validClusterWriteCommitLedgerPhase(record.Phase) {
		return ErrClusterWriteCommitLedgerCorrupt
	}
	if len(record.Participants) == 0 || len(record.Participants) > MaxClusterWriteCommitNodes {
		return ErrClusterWriteCommitLedgerCorrupt
	}
	seen := make(map[string]struct{}, len(record.Participants))
	for _, participant := range record.Participants {
		if participant.Node == "" || strings.TrimSpace(participant.Node) != participant.Node {
			return ErrClusterWriteCommitLedgerCorrupt
		}
		if _, exists := seen[participant.Node]; exists || (participant.Aborted && participant.Committed) {
			return ErrClusterWriteCommitLedgerCorrupt
		}
		seen[participant.Node] = struct{}{}
	}
	return nil
}

func validClusterWriteCommitLedgerPhase(phase ClusterWriteCommitLedgerPhase) bool {
	switch phase {
	case ClusterWriteCommitLedgerPreparing,
		ClusterWriteCommitLedgerPrepared,
		ClusterWriteCommitLedgerCommitting,
		ClusterWriteCommitLedgerCommitted,
		ClusterWriteCommitLedgerAborted,
		ClusterWriteCommitLedgerOutcomeUnknown:
		return true
	default:
		return false
	}
}

func clusterWriteCommitLedgerParticipantsMatch(participants []ClusterWriteCommitLedgerParticipant, nodes []string) bool {
	if len(participants) != len(nodes) {
		return false
	}
	for index, node := range nodes {
		if participants[index].Node != node {
			return false
		}
	}
	return true
}

func clusterWriteCommitLedgerParticipant(record *ClusterWriteCommitLedgerRecord, node string) (*ClusterWriteCommitLedgerParticipant, bool) {
	for index := range record.Participants {
		if record.Participants[index].Node == strings.TrimSpace(node) {
			return &record.Participants[index], true
		}
	}
	return nil, false
}

func cloneClusterWriteCommitLedgerRecord(record ClusterWriteCommitLedgerRecord) ClusterWriteCommitLedgerRecord {
	record.Participants = append([]ClusterWriteCommitLedgerParticipant(nil), record.Participants...)
	return record
}

func clusterWriteCommitResultFromLedger(record ClusterWriteCommitLedgerRecord) ClusterWriteCommitResult {
	result := ClusterWriteCommitResult{
		Proposal:       record.Proposal,
		Attempts:       make([]ClusterWriteCommitAttempt, len(record.Participants)),
		Committed:      record.Phase == ClusterWriteCommitLedgerCommitted,
		OutcomeUnknown: record.Phase == ClusterWriteCommitLedgerOutcomeUnknown,
	}
	for index, participant := range record.Participants {
		result.Attempts[index] = ClusterWriteCommitAttempt{
			Node:      participant.Node,
			Prepared:  participant.Prepared,
			Aborted:   participant.Aborted,
			Committed: participant.Committed,
		}
		if participant.Prepared {
			result.PreparedCount++
		}
		if participant.Aborted {
			result.AbortedCount++
		}
		if participant.Committed {
			result.CommittedCount++
		}
	}
	result.Prepared = result.PreparedCount == len(result.Attempts) && record.Phase != ClusterWriteCommitLedgerPreparing
	return result
}
