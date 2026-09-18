package hatSql

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
)

var (
	// ErrDebeziumKafkaDecoderInvalid reports invalid decoder limits.
	ErrDebeziumKafkaDecoderInvalid = errors.New("Debezium Kafka decoder is invalid")
	// ErrDebeziumKafkaEnvelopeInvalid reports a malformed or unsupported event.
	ErrDebeziumKafkaEnvelopeInvalid = errors.New("Debezium Kafka envelope is invalid")
	// ErrDebeziumKafkaKeyInvalid reports a missing or malformed record key.
	ErrDebeziumKafkaKeyInvalid = errors.New("Debezium Kafka key is invalid")
	// ErrDebeziumKafkaPayloadTooLarge reports a payload over the configured limit.
	ErrDebeziumKafkaPayloadTooLarge = errors.New("Debezium Kafka payload is too large")
)

// DebeziumKafkaTableDecoderOptions bounds the input accepted by a Debezium
// JSON decoder. Zero values use the same limits as KafkaTableSource.
type DebeziumKafkaTableDecoderOptions struct {
	MaxPayloadBytes int
	MaxKeyBytes     int
}

type debeziumKafkaRawEnvelope struct {
	Before    Row             `json:"before"`
	After     Row             `json:"after"`
	Operation string          `json:"op"`
	Payload   json.RawMessage `json:"payload"`
}

type debeziumKafkaEnvelope struct {
	Before    Row
	After     Row
	Operation string
}

// NewDebeziumKafkaTableDecoder returns a Kafka decoder for the JSON event
// shape emitted by Debezium. It converts create/read/update events into
// KafkaTableUpsert and delete events into KafkaTableDelete, while preserving
// the Kafka record key as the table key.
func NewDebeziumKafkaTableDecoder(options DebeziumKafkaTableDecoderOptions) (KafkaTableDecoder, error) {
	maxPayloadBytes := options.MaxPayloadBytes
	if maxPayloadBytes == 0 {
		maxPayloadBytes = MaxKafkaTableMessageBytes
	}
	if maxPayloadBytes < 1 || maxPayloadBytes > MaxKafkaTableMessageBytes {
		return nil, fmt.Errorf("%w: max payload bytes %d", ErrDebeziumKafkaDecoderInvalid, maxPayloadBytes)
	}
	maxKeyBytes := options.MaxKeyBytes
	if maxKeyBytes == 0 {
		maxKeyBytes = MaxKafkaTableSourceKeyBytes
	}
	if maxKeyBytes < 1 || maxKeyBytes > MaxKafkaTableSourceKeyBytes {
		return nil, fmt.Errorf("%w: max key bytes %d", ErrDebeziumKafkaDecoderInvalid, maxKeyBytes)
	}
	return func(message KafkaTableMessage) (KafkaTableChange, error) {
		return decodeDebeziumKafkaMessage(message, maxPayloadBytes, maxKeyBytes)
	}, nil
}

// DebeziumKafkaJSONDecoder is the default-limit convenience decoder.
func DebeziumKafkaJSONDecoder(message KafkaTableMessage) (KafkaTableChange, error) {
	return decodeDebeziumKafkaMessage(message, MaxKafkaTableMessageBytes, MaxKafkaTableSourceKeyBytes)
}

func decodeDebeziumKafkaMessage(message KafkaTableMessage, maxPayloadBytes, maxKeyBytes int) (KafkaTableChange, error) {
	key, err := normalizeDebeziumKafkaKey(message.Key, maxKeyBytes)
	if err != nil {
		return KafkaTableChange{}, err
	}
	if message.Value == nil {
		return KafkaTableChange{Key: key, Operation: KafkaTableDelete}, nil
	}
	if len(message.Value) > maxPayloadBytes {
		return KafkaTableChange{}, fmt.Errorf("%w: %d > %d bytes", ErrDebeziumKafkaPayloadTooLarge, len(message.Value), maxPayloadBytes)
	}
	envelope, err := parseDebeziumKafkaEnvelope(message.Value)
	if err != nil {
		return KafkaTableChange{}, err
	}
	normalized, err := NormalizeCDCEnvelope(CDCEnvelope{
		Operation: envelope.Operation,
		Key:       key,
		Before:    envelope.Before,
		After:     envelope.After,
	})
	if err != nil {
		return KafkaTableChange{}, fmt.Errorf("%w: %v", ErrDebeziumKafkaEnvelopeInvalid, err)
	}
	switch normalized.Operation {
	case CDCOperationInsert, CDCOperationUpdate:
		return KafkaTableChange{Key: normalized.Key, Operation: KafkaTableUpsert, Row: normalized.After}, nil
	case CDCOperationDelete:
		return KafkaTableChange{Key: normalized.Key, Operation: KafkaTableDelete}, nil
	default:
		return KafkaTableChange{}, fmt.Errorf("%w: operation %q", ErrDebeziumKafkaEnvelopeInvalid, normalized.Operation)
	}
}

func parseDebeziumKafkaEnvelope(data []byte) (debeziumKafkaEnvelope, error) {
	var envelope debeziumKafkaRawEnvelope
	if err := json.Unmarshal(data, &envelope); err != nil {
		return debeziumKafkaEnvelope{}, fmt.Errorf("%w: decode JSON: %v", ErrDebeziumKafkaEnvelopeInvalid, err)
	}
	if strings.TrimSpace(envelope.Operation) == "" && len(envelope.Payload) > 0 && !bytes.Equal(bytes.TrimSpace(envelope.Payload), []byte("null")) {
		var payload debeziumKafkaRawEnvelope
		if err := json.Unmarshal(envelope.Payload, &payload); err != nil {
			return debeziumKafkaEnvelope{}, fmt.Errorf("%w: decode payload: %v", ErrDebeziumKafkaEnvelopeInvalid, err)
		}
		envelope = payload
	}
	if strings.TrimSpace(envelope.Operation) == "" {
		return debeziumKafkaEnvelope{}, fmt.Errorf("%w: op is required", ErrDebeziumKafkaEnvelopeInvalid)
	}
	return debeziumKafkaEnvelope{Before: envelope.Before, After: envelope.After, Operation: envelope.Operation}, nil
}

func normalizeDebeziumKafkaKey(key string, maxBytes int) (string, error) {
	key = strings.TrimSpace(key)
	if key == "" {
		return "", fmt.Errorf("%w: key is required", ErrDebeziumKafkaKeyInvalid)
	}
	if len(key) > maxBytes {
		return "", fmt.Errorf("%w: key exceeds %d bytes", ErrDebeziumKafkaKeyInvalid, maxBytes)
	}
	if strings.ContainsAny(key[:1], "{[\"") {
		decoder := json.NewDecoder(strings.NewReader(key))
		decoder.UseNumber()
		var value interface{}
		if err := decoder.Decode(&value); err != nil {
			return "", fmt.Errorf("%w: decode JSON key: %v", ErrDebeziumKafkaKeyInvalid, err)
		}
		var trailing interface{}
		if err := decoder.Decode(&trailing); err != io.EOF {
			if err == nil {
				return "", fmt.Errorf("%w: trailing JSON key data", ErrDebeziumKafkaKeyInvalid)
			}
			return "", fmt.Errorf("%w: decode trailing JSON key data: %v", ErrDebeziumKafkaKeyInvalid, err)
		}
		canonical, err := json.Marshal(value)
		if err != nil {
			return "", fmt.Errorf("%w: encode JSON key: %v", ErrDebeziumKafkaKeyInvalid, err)
		}
		key = string(canonical)
	}
	if err := validateKafkaTableKey(key); err != nil || len(key) > maxBytes {
		return "", fmt.Errorf("%w: key exceeds configured limit", ErrDebeziumKafkaKeyInvalid)
	}
	return key, nil
}
