package hatReplication

import (
	"errors"
	"fmt"
)

var (
	ErrConflictHookRejected = errors.New("hatriecache: conflict hook rejected remote version")
	ErrConflictHookDecision = errors.New("hatriecache: conflict hook returned an invalid decision")
)

// ConflictHookContext identifies the two competing writes. KeyDigest is
// caller-supplied so hooks can correlate a key without requiring raw key data;
// Local and Remote carry the writer source/node and per-source sequence.
type ConflictHookContext struct {
	Space     string
	KeyDigest [32]byte
	Local     ConflictVersion
	Remote    ConflictVersion
}

// ConflictHookDecision controls how a hook resolves a conflict.
type ConflictHookDecision uint8

const (
	// ConflictHookUsePolicy continues with the configured conflict policy.
	ConflictHookUsePolicy ConflictHookDecision = iota
	// ConflictHookKeepLocal preserves the local version.
	ConflictHookKeepLocal
	// ConflictHookAcceptRemote applies the remote version.
	ConflictHookAcceptRemote
	// ConflictHookReject rejects the remote version.
	ConflictHookReject
)

// ConflictHook runs only when Local and Remote are distinct valid versions.
// Returning an error aborts resolution without selecting a winner.
type ConflictHook func(ConflictHookContext) (ConflictHookDecision, error)

func resolveConflictWithPolicyContext(policy ConflictPolicy, context ConflictHookContext) (ConflictVersion, error) {
	comparison, err := CompareConflictVersions(context.Local, context.Remote)
	if err != nil {
		return ConflictVersion{}, err
	}
	if comparison == 0 {
		return context.Local, nil
	}
	if policy.Hook != nil {
		decision, err := policy.Hook(context)
		if err != nil {
			return ConflictVersion{}, err
		}
		switch decision {
		case ConflictHookUsePolicy:
		case ConflictHookKeepLocal:
			return context.Local, nil
		case ConflictHookAcceptRemote:
			return context.Remote, nil
		case ConflictHookReject:
			return ConflictVersion{}, ErrConflictHookRejected
		default:
			return ConflictVersion{}, fmt.Errorf("%w: %d", ErrConflictHookDecision, decision)
		}
	}
	return resolveConflictWithPolicy(policy, context.Local, context.Remote)
}
