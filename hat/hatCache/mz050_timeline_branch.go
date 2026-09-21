package hatCache

import (
	"errors"
	"fmt"
	"sync"
)

var (
	ErrNilCommandJournalBranch    = errors.New("hatriecache: command journal branch is nil")
	ErrCommandJournalBranchClosed = errors.New("hatriecache: command journal branch is closed")
	ErrCommandJournalBranchReplay = errors.New("hatriecache: command journal branch replay failed")
)

// CommandJournalBranch is an isolated, in-memory what-if timeline rooted at
// one committed journal sequence. It never appends to the source journal or
// mutates the source trie. Successful commands are retained so the branch can
// be replayed into another empty trie while the source journal remains open.
type CommandJournalBranch struct {
	mu           sync.Mutex
	journal      *CommandJournal
	trie         *HatTrie
	baseSequence uint64
	requests     []CacheCommandRequest
	closed       bool
}

// BranchAt creates an isolated branch at targetSequence. A zero target uses
// the current journal tail. The journal must retain the requested replay
// prefix; a compacted prefix cannot be branched without its snapshot.
func (journal *CommandJournal) BranchAt(targetSequence uint64) (*CommandJournalBranch, error) {
	if journal == nil {
		return nil, ErrNilCommandJournal
	}
	trie := CreateHatTrie()
	baseSequence, err := journal.ReplayThrough(trie, 0, targetSequence)
	if err != nil {
		trie.Destroy()
		return nil, err
	}
	return &CommandJournalBranch{
		journal:      journal,
		trie:         trie,
		baseSequence: baseSequence,
	}, nil
}

// BaseSequence returns the committed journal sequence from which the branch
// was created. It remains available after Close for diagnostics.
func (branch *CommandJournalBranch) BaseSequence() uint64 {
	if branch == nil {
		return 0
	}
	branch.mu.Lock()
	defer branch.mu.Unlock()
	return branch.baseSequence
}

// CommandCount returns the number of successful commands recorded after the
// branch base. It is safe to call while the branch is being updated.
func (branch *CommandJournalBranch) CommandCount() int {
	if branch == nil {
		return 0
	}
	branch.mu.Lock()
	defer branch.mu.Unlock()
	return len(branch.requests)
}

// Execute applies one hypothetical command to the branch. Successful
// requests are cloned before retention so callers may reuse their input
// buffers. Failed commands do not become part of the branch timeline.
func (branch *CommandJournalBranch) Execute(request CacheCommandRequest) CacheCommandResponse {
	if branch == nil {
		return CacheCommandResponse{Message: ErrNilCommandJournalBranch.Error()}
	}
	branch.mu.Lock()
	defer branch.mu.Unlock()
	if branch.closed {
		return CacheCommandResponse{Message: ErrCommandJournalBranchClosed.Error()}
	}
	response := branch.trie.ExecuteCommand(request)
	if response.OK {
		branch.requests = append(branch.requests, cloneAsyncCommandRequest(request))
	}
	return response
}

// Requests returns an independently owned copy of the successful hypothetical
// commands recorded after the branch base.
func (branch *CommandJournalBranch) Requests() []CacheCommandRequest {
	if branch == nil {
		return nil
	}
	branch.mu.Lock()
	defer branch.mu.Unlock()
	if branch.closed {
		return nil
	}
	requests := make([]CacheCommandRequest, len(branch.requests))
	for index, request := range branch.requests {
		requests[index] = cloneAsyncCommandRequest(request)
	}
	return requests
}

// ReplayInto rebuilds the branch in target by replaying its journal prefix and
// then applying the retained hypothetical commands in order. The target must
// be empty and is caller-owned. It is safe to call concurrently with Execute;
// the replay uses the branch request snapshot captured at method entry.
func (branch *CommandJournalBranch) ReplayInto(target *HatTrie) error {
	if branch == nil {
		return ErrNilCommandJournalBranch
	}
	if target == nil {
		return ErrNilHatTrie
	}
	branch.mu.Lock()
	if branch.closed {
		branch.mu.Unlock()
		return ErrCommandJournalBranchClosed
	}
	journal := branch.journal
	baseSequence := branch.baseSequence
	requests := make([]CacheCommandRequest, len(branch.requests))
	for index, request := range branch.requests {
		requests[index] = cloneAsyncCommandRequest(request)
	}
	branch.mu.Unlock()
	if journal == nil {
		return ErrNilCommandJournal
	}
	if _, err := journal.ReplayThrough(target, 0, baseSequence); err != nil {
		return err
	}
	for _, request := range requests {
		response := target.ExecuteCommand(request)
		if !response.OK {
			return fmt.Errorf("%w: command %q: %s", ErrCommandJournalBranchReplay, request.Command, response.Message)
		}
	}
	return nil
}

// Close releases the isolated trie. It is idempotent; closing the branch does
// not close or alter the source journal.
func (branch *CommandJournalBranch) Close() {
	if branch == nil {
		return
	}
	branch.mu.Lock()
	if branch.closed {
		branch.mu.Unlock()
		return
	}
	branch.closed = true
	trie := branch.trie
	branch.trie = nil
	branch.requests = nil
	branch.mu.Unlock()
	if trie != nil {
		trie.Destroy()
	}
}
