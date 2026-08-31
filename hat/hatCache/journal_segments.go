package hatCache

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"hatrie_cache/hat/hatJournal"
)

type commandJournalSegment struct {
	path  string
	start uint64
	end   uint64
}

func (journal *CommandJournal) segmented() bool {
	return journal != nil && journal.segmentMaxBytes > 0
}

func commandJournalSegmentDir(path string) string {
	return hatJournal.SegmentDirectory(path)
}

func commandJournalSegmentPath(path string, start uint64, end uint64) string {
	return hatJournal.SegmentPath(path, start, end)
}

func listCommandJournalSegments(path string) ([]commandJournalSegment, error) {
	portable, err := hatJournal.ListSegments(path)
	if err != nil {
		return nil, err
	}
	segments := make([]commandJournalSegment, len(portable))
	for index, segment := range portable {
		segments[index] = commandJournalSegment{path: segment.Path, start: segment.Start, end: segment.End}
	}
	return segments, nil
}

func scanCommandJournalSet(path string, segmented bool, visit func(commandJournalEntry) error) (int64, error) {
	if !segmented {
		return scanCommandJournalEntries(path, visit)
	}
	segments, err := listCommandJournalSegments(path)
	if err != nil {
		return 0, err
	}
	files := make([]string, 0, len(segments)+1)
	for _, segment := range segments {
		files = append(files, segment.path)
	}
	files = append(files, path)

	var previousSequence uint64
	var hasPreviousSequence bool
	var activeValidBytes int64
	for fileIndex, filePath := range files {
		firstEntry := true
		var firstMutation uint64
		var lastMutation uint64
		validBytes, err := scanCommandJournalEntries(filePath, func(entry commandJournalEntry) error {
			if firstEntry && fileIndex > 0 && entry.Checkpoint {
				firstEntry = false
				if !hasPreviousSequence || entry.Sequence != previousSequence {
					return fmt.Errorf("hatriecache: journal segment checkpoint %d does not continue after %d", entry.Sequence, previousSequence)
				}
				return nil
			}
			firstEntry = false
			if !entry.Checkpoint {
				if firstMutation == 0 {
					firstMutation = entry.Sequence
				}
				lastMutation = entry.Sequence
			}
			if err := validateCommandJournalEntrySequence(previousSequence, hasPreviousSequence, entry); err != nil {
				return err
			}
			if visit != nil {
				if err := visit(entry); err != nil {
					return err
				}
			}
			previousSequence = entry.Sequence
			hasPreviousSequence = true
			return nil
		})
		if err != nil {
			return 0, err
		}
		if fileIndex < len(segments) {
			segment := segments[fileIndex]
			if firstMutation != segment.start || lastMutation != segment.end {
				return 0, fmt.Errorf("hatriecache: journal segment %q bounds do not match records %d-%d", filepath.Base(filePath), firstMutation, lastMutation)
			}
			info, err := os.Stat(filePath)
			if err != nil {
				return 0, err
			}
			if validBytes != info.Size() {
				return 0, fmt.Errorf("hatriecache: archived journal segment %q is truncated", filepath.Base(filePath))
			}
		} else {
			activeValidBytes = validBytes
		}
	}
	return activeValidBytes, nil
}

func commandJournalActiveSegmentStart(path string, lastSequence uint64) (uint64, error) {
	start := uint64(1)
	if lastSequence > 0 && lastSequence < ^uint64(0) {
		start = lastSequence + 1
	} else if lastSequence == ^uint64(0) {
		start = lastSequence
	}
	first := true
	if _, err := scanCommandJournalEntries(path, func(entry commandJournalEntry) error {
		if !first {
			return nil
		}
		first = false
		if entry.Checkpoint {
			if entry.Sequence == ^uint64(0) {
				start = entry.Sequence
			} else {
				start = entry.Sequence + 1
			}
		} else {
			start = entry.Sequence
		}
		return nil
	}); err != nil {
		return 0, err
	}
	return start, nil
}

func (journal *CommandJournal) rotateSegmentIfFullLocked() error {
	if !journal.segmented() || journal.file == nil {
		return nil
	}
	info, err := journal.file.Stat()
	if err != nil {
		return err
	}
	if info.Size() < journal.segmentMaxBytes {
		return nil
	}
	return journal.rotateSegmentLocked(false)
}

func (journal *CommandJournal) rotateSegmentLocked(force bool) error {
	if !journal.segmented() {
		return nil
	}
	if err := journal.ensureAppendFileLocked(); err != nil {
		return err
	}
	lastSequence := journal.lastSequenceLocked()
	if journal.activeSegmentStart == 0 {
		journal.activeSegmentStart = 1
	}
	if journal.activeSegmentStart > lastSequence || lastSequence == 0 {
		return nil
	}
	if !force {
		info, err := journal.file.Stat()
		if err != nil {
			return err
		}
		if info.Size() < journal.segmentMaxBytes {
			return nil
		}
	}

	start := journal.activeSegmentStart
	segmentDir := commandJournalSegmentDir(journal.path)
	if err := os.MkdirAll(segmentDir, 0o700); err != nil {
		return err
	}
	destination := commandJournalSegmentPath(journal.path, start, lastSequence)
	if _, err := os.Stat(destination); err == nil {
		return fmt.Errorf("hatriecache: command journal segment already exists: %s", filepath.Base(destination))
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := journal.closeAppendFileLocked(); err != nil {
		return err
	}
	if err := os.Rename(journal.path, destination); err != nil {
		if reopenErr := journal.ensureAppendFileLocked(); reopenErr != nil {
			return errors.Join(err, reopenErr)
		}
		return err
	}
	journal.activeSegmentStart = journal.nextSequence
	if err := syncDirectory(segmentDir); err != nil {
		return journal.recoverActiveFileAfterRotationLocked(err)
	}
	if err := syncDirectory(filepath.Dir(journal.path)); err != nil {
		return journal.recoverActiveFileAfterRotationLocked(err)
	}
	if err := journal.ensureAppendFileLocked(); err != nil {
		return err
	}
	if err := journal.syncLocked(); err != nil {
		return err
	}
	if err := syncDirectory(filepath.Dir(journal.path)); err != nil {
		return err
	}
	return journal.pruneSegmentsLocked()
}

func (journal *CommandJournal) recoverActiveFileAfterRotationLocked(cause error) error {
	if err := journal.ensureAppendFileLocked(); err != nil {
		return errors.Join(cause, err)
	}
	return cause
}

func (journal *CommandJournal) writeCheckpointWithoutSyncLocked(sequence uint64) error {
	if sequence == 0 {
		return nil
	}
	data, err := marshalCommandJournalEntry(commandJournalEntry{
		Version:    commandJournalVersion,
		Sequence:   sequence,
		Checkpoint: true,
	}, journal.format)
	if err != nil {
		return err
	}
	n, err := journal.file.Write(data)
	if err != nil {
		return err
	}
	if n != len(data) {
		return io.ErrShortWrite
	}
	return nil
}

func (journal *CommandJournal) pruneSegmentsLocked() error {
	if !journal.segmented() || journal.retainedSegments <= 0 {
		return nil
	}
	segments, err := listCommandJournalSegments(journal.path)
	if err != nil {
		return err
	}
	remove := len(segments) - journal.retainedSegments
	if remove <= 0 {
		return nil
	}
	for _, segment := range segments[:remove] {
		if journal.outboxRetainFrom > 0 && segment.end >= journal.outboxRetainFrom {
			break
		}
		if projectionThrough, protected := journal.projectionRetentionThroughLocked(); protected && segment.end > projectionThrough {
			break
		}
		if err := os.Remove(segment.path); err != nil {
			return err
		}
	}
	return syncDirectory(commandJournalSegmentDir(journal.path))
}

func readCommandJournalTailSet(path string, segmented bool, afterSequence uint64, limit int) (CommandJournalTail, error) {
	tail := CommandJournalTail{Entries: []CommandJournalRecord{}}
	if limit > 0 {
		tail.Limit = limit
		tail.Entries = make([]CommandJournalRecord, 0, limit)
	}
	if _, err := scanCommandJournalSet(path, segmented, func(entry commandJournalEntry) error {
		if entry.Sequence > tail.LastSequence {
			tail.LastSequence = entry.Sequence
		}
		if entry.Checkpoint && entry.Sequence > tail.CompactedThrough {
			tail.CompactedThrough = entry.Sequence
		}
		if entry.Checkpoint || entry.Sequence <= afterSequence {
			return nil
		}
		if limit > 0 && len(tail.Entries) >= limit {
			tail.HasMore = true
			return nil
		}
		tail.Entries = append(tail.Entries, CommandJournalRecord{Sequence: entry.Sequence, Request: entry.Request})
		return nil
	}); err != nil {
		return CommandJournalTail{}, err
	}
	return tail, nil
}
