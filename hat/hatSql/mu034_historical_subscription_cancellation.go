package hatSql

import (
	"context"
	"errors"
	"fmt"
)

// SQLPublicationSubscriptionState is the durable lifecycle state of a
// historical publication consumer.
type SQLPublicationSubscriptionState string

const (
	SQLPublicationSubscriptionStateActive    SQLPublicationSubscriptionState = "active"
	SQLPublicationSubscriptionStateCancelled SQLPublicationSubscriptionState = "cancelled"
	SQLPublicationSubscriptionStateCompleted SQLPublicationSubscriptionState = "completed"
)

var (
	// ErrSQLPublicationSubscriptionCancelled means a durable subscription was
	// explicitly cancelled and cannot be resumed under the same ID.
	ErrSQLPublicationSubscriptionCancelled = errors.New("hatSql: SQL publication subscription cancelled")
	// ErrSQLPublicationSubscriptionCompleted means a durable subscription has
	// acknowledged its terminal publication batch.
	ErrSQLPublicationSubscriptionCompleted = errors.New("hatSql: SQL publication subscription completed")
	// ErrSQLPublicationSubscriptionStore means a durable lifecycle or
	// checkpoint record could not be committed.
	ErrSQLPublicationSubscriptionStore = errors.New("hatSql: SQL publication subscription store commit failed")
	// ErrSQLPublicationSubscriptionDuplicate means the same durable ID is
	// already active in this publication process.
	ErrSQLPublicationSubscriptionDuplicate = errors.New("hatSql: SQL publication subscription is already active")
	// ErrSQLPublicationSubscriptionStoreRequired means durable subscriptions
	// were requested without a checkpoint store.
	ErrSQLPublicationSubscriptionStoreRequired = errors.New("hatSql: SQL publication subscription store is required")
)

// SQLPublicationSubscriptionRecord is the single durable state record for a
// publication consumer. A store must replace the record atomically so a
// restart sees either the previous checkpoint or the new one.
type SQLPublicationSubscriptionRecord struct {
	PublicationName string                          `json:"publication_name"`
	SubscriptionID  string                          `json:"subscription_id"`
	Checkpoint      SQLPublicationCheckpoint        `json:"checkpoint"`
	State           SQLPublicationSubscriptionState `json:"state"`
}

// SQLPublicationSubscriptionStore persists durable replay state. Load must
// return false when no record exists. Commit must be atomic with respect to a
// single publication/subscription ID.
type SQLPublicationSubscriptionStore interface {
	Load(context.Context, string, string) (SQLPublicationSubscriptionRecord, bool, error)
	Commit(context.Context, SQLPublicationSubscriptionRecord) error
}

// SQLPublicationDurableSubscriptionOptions configures a restartable
// historical publication consumer. Checkpoint is used only when the store has
// no record for SubscriptionID; subsequent resumes always use the stored
// checkpoint.
type SQLPublicationDurableSubscriptionOptions struct {
	SubscriptionID string
	Store          SQLPublicationSubscriptionStore
	Checkpoint     SQLPublicationCheckpoint
}

// SubscribeDurable creates a restartable replay-plus-live subscription. An
// active store record resumes after its acknowledged checkpoint. Close is a
// resumable disconnect; Cancel durably terminates the ID.
func (publication *SQLPublication) SubscribeDurable(ctx context.Context, options SQLPublicationDurableSubscriptionOptions) (*SQLPublicationSubscription, error) {
	if publication == nil {
		return nil, fmt.Errorf("%w: nil publication", ErrSQLPublicationInvalid)
	}
	if options.Store == nil {
		return nil, ErrSQLPublicationSubscriptionStoreRequired
	}
	if err := validateSQLPublicationText(options.SubscriptionID, maxSQLPublicationTextBytes, "subscription ID"); err != nil {
		return nil, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	record, found, err := options.Store.Load(ctx, publication.name, options.SubscriptionID)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrSQLPublicationSubscriptionStore, err)
	}
	checkpoint := options.Checkpoint
	if found {
		if record.PublicationName != publication.name || record.SubscriptionID != options.SubscriptionID {
			return nil, fmt.Errorf("%w: durable record identity mismatch", ErrSQLPublicationInvalid)
		}
		switch record.State {
		case SQLPublicationSubscriptionStateActive:
			checkpoint = record.Checkpoint
		case SQLPublicationSubscriptionStateCancelled:
			return nil, ErrSQLPublicationSubscriptionCancelled
		case SQLPublicationSubscriptionStateCompleted:
			return nil, ErrSQLPublicationSubscriptionCompleted
		default:
			return nil, fmt.Errorf("%w: unknown durable subscription state %q", ErrSQLPublicationInvalid, record.State)
		}
	}

	subscription, err := publication.Subscribe(ctx, checkpoint)
	if err != nil {
		return nil, err
	}
	publication.mu.Lock()
	if existing := publication.durableSubscriptions[options.SubscriptionID]; existing != nil && !existing.closed {
		publication.mu.Unlock()
		subscription.Close()
		return nil, ErrSQLPublicationSubscriptionDuplicate
	}
	subscription.durableStore = options.Store
	subscription.durableID = options.SubscriptionID
	publication.durableSubscriptions[options.SubscriptionID] = subscription.state
	publication.mu.Unlock()

	if !found {
		initial := SQLPublicationSubscriptionRecord{
			PublicationName: publication.name,
			SubscriptionID:  options.SubscriptionID,
			Checkpoint:      checkpoint,
			State:           SQLPublicationSubscriptionStateActive,
		}
		if err := options.Store.Commit(ctx, initial); err != nil {
			subscription.Close()
			return nil, fmt.Errorf("%w: %v", ErrSQLPublicationSubscriptionStore, err)
		}
	}
	return subscription, nil
}

// Cancel durably marks a subscription terminal before closing its channels.
// A failed store commit leaves the subscription active so the caller can
// retry without losing its checkpoint.
func (subscription *SQLPublicationSubscription) Cancel(ctx context.Context) error {
	if subscription == nil || subscription.publication == nil || subscription.state == nil {
		return fmt.Errorf("%w: nil subscription", ErrSQLPublicationInvalid)
	}
	if subscription.durableStore == nil {
		return ErrSQLPublicationSubscriptionStoreRequired
	}
	if ctx == nil {
		ctx = context.Background()
	}
	publication := subscription.publication
	publication.mu.Lock()
	defer publication.mu.Unlock()
	if subscription.state.closed && errors.Is(subscription.state.err, ErrSQLPublicationSubscriptionCancelled) {
		return nil
	}
	if subscription.state.closed && subscription.state.completed {
		return ErrSQLPublicationSubscriptionCompleted
	}
	record := SQLPublicationSubscriptionRecord{
		PublicationName: publication.name,
		SubscriptionID:  subscription.durableID,
		Checkpoint:      subscription.state.checkpoint,
		State:           SQLPublicationSubscriptionStateCancelled,
	}
	if err := subscription.durableStore.Commit(ctx, record); err != nil {
		return fmt.Errorf("%w: %v", ErrSQLPublicationSubscriptionStore, err)
	}
	if !subscription.state.closed {
		publication.closeSubscriberLocked(subscription.state, ErrSQLPublicationSubscriptionCancelled)
	} else {
		subscription.state.err = ErrSQLPublicationSubscriptionCancelled
	}
	delete(publication.subscribers, subscription.state.id)
	return nil
}
