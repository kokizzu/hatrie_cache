package hatSql

import (
	"errors"
	"fmt"
)

const querySubscriptionCheckpointVersion uint8 = 1

var (
	// ErrQuerySubscriptionNotAcknowledged means no consumer-applied result is
	// available for a durable checkpoint yet.
	ErrQuerySubscriptionNotAcknowledged = errors.New("query subscription has no acknowledged snapshot")
	// ErrQuerySubscriptionCheckpointInvalid identifies a checkpoint that cannot
	// be resumed without risking a duplicate or missing result.
	ErrQuerySubscriptionCheckpointInvalid = errors.New("invalid query subscription checkpoint")
	// ErrQuerySubscriptionCheckpointComplete identifies a bounded subscription
	// whose historical replay already reached its terminal frontier.
	ErrQuerySubscriptionCheckpointComplete = errors.New("query subscription checkpoint is complete")
)

// QuerySubscriptionCheckpoint is the consumer-applied state needed to resume
// one historical query subscription. The caller owns durable storage for this
// value; Resume never rereads the source at the checkpoint frontier.
type QuerySubscriptionCheckpoint struct {
	Version      uint8
	Definition   QuerySubscriptionDefinition
	Snapshot     QuerySubscriptionSnapshot
	Differential bool
}

type querySubscriptionCheckpointState struct {
	snapshot QuerySubscriptionSnapshot
}

// Acknowledge records the latest snapshot fully applied by the consumer. It
// must refer to the subscription's current revision and frontier so a stale
// queued update cannot advance a durable checkpoint past work the consumer
// did not apply.
func (subscription *QuerySubscription) Acknowledge(snapshot QuerySubscriptionSnapshot) error {
	if subscription == nil {
		return ErrQuerySubscriptionCheckpointInvalid
	}
	subscription.mu.Lock()
	defer subscription.mu.Unlock()
	if subscription.closed {
		return ErrQuerySubscriptionCheckpointInvalid
	}
	if subscription.differentialUpdates != nil {
		return ErrQuerySubscriptionCheckpointInvalid
	}
	if err := subscription.validateAcknowledgementLocked(snapshot); err != nil {
		return err
	}
	acknowledged := cloneQuerySubscriptionSnapshot(subscription.snapshot)
	acknowledged.Progress = false
	subscription.checkpoint = &querySubscriptionCheckpointState{snapshot: acknowledged}
	return nil
}

func (subscription *QuerySubscription) acknowledgeDifferential(batch QuerySubscriptionDeltaBatch) error {
	if subscription == nil {
		return ErrQuerySubscriptionCheckpointInvalid
	}
	subscription.mu.Lock()
	defer subscription.mu.Unlock()
	if subscription.closed || subscription.differentialUpdates == nil {
		return ErrQuerySubscriptionCheckpointInvalid
	}
	if batch.ID != subscription.snapshot.ID || batch.Revision != subscription.snapshot.Revision || batch.Frontier != subscription.snapshot.Frontier || batch.Complete != subscription.snapshot.Complete {
		return fmt.Errorf("%w: differential batch does not identify the current revision", ErrQuerySubscriptionCheckpointInvalid)
	}
	acknowledged := cloneQuerySubscriptionSnapshot(subscription.snapshot)
	acknowledged.Progress = false
	subscription.checkpoint = &querySubscriptionCheckpointState{snapshot: acknowledged}
	return nil
}

func (subscription *QuerySubscription) validateAcknowledgementLocked(snapshot QuerySubscriptionSnapshot) error {
	if snapshot.ID != subscription.snapshot.ID || snapshot.Revision != subscription.snapshot.Revision || snapshot.Frontier != subscription.snapshot.Frontier || snapshot.Complete != subscription.snapshot.Complete {
		return fmt.Errorf("%w: snapshot does not identify the current revision", ErrQuerySubscriptionCheckpointInvalid)
	}
	if !snapshot.Progress && !sameQuerySubscriptionResult(snapshot.Result, subscription.snapshot.Result) {
		return fmt.Errorf("%w: snapshot result differs from the current revision", ErrQuerySubscriptionCheckpointInvalid)
	}
	return nil
}

// Checkpoint returns a detached checkpoint for the latest acknowledged
// consumer state. A producer update that is still queued is intentionally not
// included.
func (subscription *QuerySubscription) Checkpoint() (QuerySubscriptionCheckpoint, error) {
	if subscription == nil {
		return QuerySubscriptionCheckpoint{}, ErrQuerySubscriptionCheckpointInvalid
	}
	subscription.mu.RLock()
	defer subscription.mu.RUnlock()
	if subscription.closed {
		return QuerySubscriptionCheckpoint{}, ErrQuerySubscriptionCheckpointInvalid
	}
	if subscription.checkpoint == nil {
		return QuerySubscriptionCheckpoint{}, ErrQuerySubscriptionNotAcknowledged
	}
	return subscription.checkpointLocked(), nil
}

// CloseWithCheckpoint atomically captures the latest acknowledged consumer
// state and closes the subscription. If no state was acknowledged, the
// subscription remains active and the error is returned.
func (subscription *QuerySubscription) CloseWithCheckpoint() (QuerySubscriptionCheckpoint, error) {
	if subscription == nil || subscription.registry == nil {
		return QuerySubscriptionCheckpoint{}, ErrQuerySubscriptionCheckpointInvalid
	}
	return subscription.registry.closeWithCheckpoint(subscription)
}

func (subscription *QuerySubscription) checkpointLocked() QuerySubscriptionCheckpoint {
	return QuerySubscriptionCheckpoint{
		Version:      querySubscriptionCheckpointVersion,
		Definition:   cloneQuerySubscriptionDefinition(subscription.definition),
		Snapshot:     cloneQuerySubscriptionSnapshot(subscription.checkpoint.snapshot),
		Differential: subscription.differentialUpdates != nil,
	}
}

func (registry *QuerySubscriptions) closeWithCheckpoint(subscription *QuerySubscription) (QuerySubscriptionCheckpoint, error) {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	subscription.mu.Lock()
	defer subscription.mu.Unlock()
	if subscription.closed || subscription.checkpoint == nil {
		if subscription.checkpoint == nil {
			return QuerySubscriptionCheckpoint{}, ErrQuerySubscriptionNotAcknowledged
		}
		return QuerySubscriptionCheckpoint{}, ErrQuerySubscriptionCheckpointInvalid
	}
	checkpoint := subscription.checkpointLocked()
	if registry.subs[subscription.id] == subscription {
		delete(registry.subs, subscription.id)
	}
	subscription.closed = true
	close(subscription.updates)
	if subscription.differentialUpdates != nil {
		close(subscription.differentialUpdates)
	}
	return checkpoint, nil
}

// Resume restores a previously acknowledged ordinary subscription without
// evaluating its query at the checkpoint frontier. The next NotifyChangedAt
// call either skips a replayed frontier or refreshes from the next frontier.
func (registry *QuerySubscriptions) Resume(checkpoint QuerySubscriptionCheckpoint) (*QuerySubscription, error) {
	subscription, err := registry.resume(checkpoint, false)
	if err != nil {
		return nil, err
	}
	return subscription, nil
}

// ResumeDifferential restores a previously acknowledged differential
// subscription. It emits no initial batch because the checkpoint represents
// the consumer's already-applied state.
func (registry *QuerySubscriptions) ResumeDifferential(checkpoint QuerySubscriptionCheckpoint) (*QueryDifferentialSubscription, error) {
	subscription, err := registry.resume(checkpoint, true)
	if err != nil {
		return nil, err
	}
	return &QueryDifferentialSubscription{subscription: subscription}, nil
}

func (registry *QuerySubscriptions) resume(checkpoint QuerySubscriptionCheckpoint, differential bool) (*QuerySubscription, error) {
	if registry == nil {
		return nil, ErrQuerySubscriptionCheckpointInvalid
	}
	if checkpoint.Version != querySubscriptionCheckpointVersion {
		return nil, fmt.Errorf("%w: version=%d", ErrQuerySubscriptionCheckpointInvalid, checkpoint.Version)
	}
	if checkpoint.Differential != differential {
		return nil, fmt.Errorf("%w: differential mode mismatch", ErrQuerySubscriptionCheckpointInvalid)
	}
	definition, err := normalizeQuerySubscriptionDefinition(checkpoint.Definition)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrQuerySubscriptionCheckpointInvalid, err)
	}
	snapshot := checkpoint.Snapshot
	if snapshot.Complete {
		return nil, ErrQuerySubscriptionCheckpointComplete
	}
	if snapshot.Frontier < definition.AsOf || definition.UpTo > 0 && snapshot.Frontier > definition.UpTo {
		return nil, fmt.Errorf("%w: frontier=%d is outside [%d,%d]", ErrQuerySubscriptionCheckpointInvalid, snapshot.Frontier, definition.AsOf, definition.UpTo)
	}
	if snapshot.Revision == 0 && !definition.StartLive {
		return nil, fmt.Errorf("%w: zero revision requires StartLive", ErrQuerySubscriptionCheckpointInvalid)
	}

	registry.mu.Lock()
	registry.nextID++
	resumedSnapshot := cloneQuerySubscriptionSnapshot(snapshot)
	resumedSnapshot.ID = registry.nextID
	resumedSnapshot.Progress = false
	subscription := &QuerySubscription{
		registry:   registry,
		id:         registry.nextID,
		definition: definition,
		snapshot:   resumedSnapshot,
		updates:    make(chan QuerySubscriptionSnapshot, registry.buffer),
	}
	subscription.checkpoint = &querySubscriptionCheckpointState{snapshot: cloneQuerySubscriptionSnapshot(resumedSnapshot)}
	if differential {
		differentialBuffer := registry.buffer
		if differentialBuffer < 2 {
			differentialBuffer = 2
		}
		subscription.differentialUpdates = make(chan QuerySubscriptionDeltaBatch, differentialBuffer)
	}
	registry.subs[subscription.id] = subscription
	registry.mu.Unlock()
	return subscription, nil
}

func cloneQuerySubscriptionDefinition(definition QuerySubscriptionDefinition) QuerySubscriptionDefinition {
	definition.Dependencies = append([]string(nil), definition.Dependencies...)
	definition.Parameters = append([]interface{}(nil), definition.Parameters...)
	return definition
}
