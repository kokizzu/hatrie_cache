package hatCache

import (
	"errors"
	"strings"
)

// executeCommandForReplay keeps the command transaction barrier used by the
// public dispatcher while avoiding response construction for common durable
// mutations. Commands outside this small set use the regular implementation.
func executeCommandForReplay(ht *HatTrie, request CacheCommandRequest) error {
	if ht == nil {
		return ErrNilHatTrie
	}
	if strings.EqualFold(strings.TrimSpace(request.Command), "BATCH") && request.Atomic {
		ht.commandTransactionMu.Lock()
		defer ht.commandTransactionMu.Unlock()
	} else {
		ht.commandTransactionMu.RLock()
		defer ht.commandTransactionMu.RUnlock()
	}

	request.Command = normalizedCommand(request.Command)
	key := strings.TrimSpace(request.Key)
	if replayFastPathCommand(request.Command) {
		if key == "" {
			return errors.New("key is required")
		}
		if err := validateKey(key); err != nil {
			return err
		}
	}
	switch request.Command {
	case "SET", "SETSTR":
		if response, ok := validateOptionalCommandExpiration(request.TTLSeconds, request.UnixSeconds); ok && !response.OK {
			return errors.New(response.Message)
		}
		if err := ht.UpsertStringChecked(key, request.Value); err != nil {
			return err
		}
		if response, ok := ht.applyCommandExpiration(key, request.TTLSeconds, request.UnixSeconds); ok && !response.OK {
			return errors.New(response.Message)
		}
		return nil
	case "SETX", "SETSTRX":
		ttl, ok := requirePositiveTTL(request.TTLSeconds)
		if !ok {
			return errors.New("positive ttl_seconds is required")
		}
		if err := ht.UpsertStringChecked(key, request.Value); err != nil {
			return err
		}
		if !ht.Expire(key, ttl) {
			return errors.New("failed to set ttl")
		}
		return nil
	case "SETINT":
		value, ok := parseCommandInt32(request.Value)
		if !ok {
			return errors.New("value must be a 32-bit integer")
		}
		if response, ok := validateOptionalCommandExpiration(request.TTLSeconds, request.UnixSeconds); ok && !response.OK {
			return errors.New(response.Message)
		}
		if err := ht.UpsertCounterChecked(key, value); err != nil {
			return err
		}
		if response, ok := ht.applyCommandExpiration(key, request.TTLSeconds, request.UnixSeconds); ok && !response.OK {
			return errors.New(response.Message)
		}
		return nil
	case "SETINTX":
		value, ok := parseCommandInt32(request.Value)
		if !ok {
			return errors.New("value must be a 32-bit integer")
		}
		ttl, ok := requirePositiveTTL(request.TTLSeconds)
		if !ok {
			return errors.New("positive ttl_seconds is required")
		}
		if err := ht.UpsertCounterChecked(key, value); err != nil {
			return err
		}
		if !ht.Expire(key, ttl) {
			return errors.New("failed to set ttl")
		}
		return nil
	case "INC":
		by, ok := parseCommandIncrement(request.Value)
		if !ok {
			return errors.New("value must be a 32-bit integer")
		}
		if _, updated, err := ht.commandIncrementCounter(key, by); err != nil {
			return err
		} else if !updated {
			return errors.New("counter overflow")
		}
		return nil
	case "DEL":
		ht.Delete(key)
		return nil
	default:
		response := ht.executeCommand(request)
		if !response.OK {
			return errors.New(response.Message)
		}
		return nil
	}
}

func replayFastPathCommand(command string) bool {
	switch command {
	case "SET", "SETSTR", "SETX", "SETSTRX", "SETINT", "SETINTX", "INC", "DEL":
		return true
	default:
		return false
	}
}
