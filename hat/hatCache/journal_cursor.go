package hatCache

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"

	"hatrie_cache/hat/hatDataStructure"
)

const (
	commandJournalCursorSchemaVersion = uint64(1)
	commandJournalCursorIndex         = "journal-tail"
	commandJournalNextCursorHeader    = "X-Hatrie-Journal-Next-Cursor"
)

var errCommandJournalCursorNotConfigured = errors.New("hatriecache: journal cursor is not configured")

type commandJournalCursorCodec struct {
	codec *hatDataStructure.CursorTokenCodec
}

func newCommandJournalCursorCodec(secret string) (*commandJournalCursorCodec, error) {
	secret = strings.TrimSpace(secret)
	if secret == "" {
		return nil, nil
	}
	codec, err := hatDataStructure.NewCursorTokenCodec([]byte(secret))
	if err != nil {
		return nil, errors.New("hatriecache: journal cursor secret is invalid")
	}
	return &commandJournalCursorCodec{codec: codec}, nil
}

func commandJournalCursorBinding(path string) []byte {
	digest := sha256.Sum256([]byte(path))
	return digest[:16]
}

func (cursor *commandJournalCursorCodec) encode(journal *CommandJournal, afterSequence uint64) (string, error) {
	if cursor == nil || cursor.codec == nil {
		return "", errCommandJournalCursorNotConfigured
	}
	if journal == nil || journal.path == "" {
		return "", ErrNilCommandJournal
	}
	if afterSequence == 0 {
		return "", fmt.Errorf("hatriecache: journal cursor sequence must be positive")
	}
	return cursor.codec.Encode(commandJournalCursorIndex, commandJournalCursorSchemaVersion, commandJournalCursorBinding(journal.path), afterSequence)
}

func (cursor *commandJournalCursorCodec) decode(journal *CommandJournal, token string) (uint64, error) {
	if cursor == nil || cursor.codec == nil {
		return 0, errCommandJournalCursorNotConfigured
	}
	if journal == nil || journal.path == "" {
		return 0, ErrNilCommandJournal
	}
	token = strings.TrimSpace(token)
	if token == "" {
		return 0, fmt.Errorf("hatriecache: journal cursor is empty")
	}
	decoded, err := cursor.codec.DecodeFor(token, commandJournalCursorIndex, commandJournalCursorSchemaVersion)
	if err != nil {
		return 0, fmt.Errorf("hatriecache: invalid journal cursor: %w", err)
	}
	if decoded.ID == 0 {
		return 0, fmt.Errorf("hatriecache: invalid journal cursor sequence")
	}
	if !bytes.Equal(decoded.Key, commandJournalCursorBinding(journal.path)) {
		return 0, fmt.Errorf("hatriecache: invalid journal cursor binding")
	}
	return decoded.ID, nil
}
