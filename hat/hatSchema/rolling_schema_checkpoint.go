package hatSchema

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

const (
	// MaxRollingSchemaCheckpointNodes bounds the number of replica phases that
	// a checkpoint decoder will allocate.
	MaxRollingSchemaCheckpointNodes = 1 << 16
	// MaxRollingSchemaCheckpointBytes bounds a persisted checkpoint frame.
	MaxRollingSchemaCheckpointBytes = 1 << 20

	rollingSchemaCheckpointVersion byte = 1
)

var (
	// ErrRollingSchemaCheckpointInvalid reports malformed, non-canonical, or
	// plan-incompatible checkpoint state.
	ErrRollingSchemaCheckpointInvalid = errors.New("hatSchema: rolling schema checkpoint is invalid")
	// ErrRollingSchemaCheckpointChecksum reports an accidentally corrupted
	// checkpoint frame. The checksum is not an authentication mechanism.
	ErrRollingSchemaCheckpointChecksum = errors.New("hatSchema: rolling schema checkpoint checksum mismatch")
)

var rollingSchemaCheckpointMagic = [...]byte{'H', 'R', 'C', '1'}
var rollingSchemaCheckpointCRCTable = crc32.MakeTable(crc32.Castagnoli)

// RollingSchemaCheckpoint is the durable, stable-phase state of one rolling
// schema deployment. The schema bodies are intentionally omitted; Restore
// requires the caller to provide the same validated plan and matches both
// schema fingerprints before accepting progress.
type RollingSchemaCheckpoint struct {
	PreviousFingerprint string              `json:"previous_fingerprint"`
	NextFingerprint     string              `json:"next_fingerprint"`
	Nodes               []RollingSchemaNode `json:"nodes"`
}

// Checkpoint snapshots a deployment for persistence and later recovery. An
// in-progress install or activation is represented by its last stable phase,
// so a restarted coordinator safely retries that hook.
func (plan RollingSchemaPlan) Checkpoint(deployment *RollingSchemaDeployment) (RollingSchemaCheckpoint, error) {
	if deployment == nil || len(plan.nodes) == 0 {
		return RollingSchemaCheckpoint{}, ErrRollingSchemaCheckpointInvalid
	}
	nodes := deployment.Snapshot()
	if len(nodes) != len(plan.nodes) {
		return RollingSchemaCheckpoint{}, fmt.Errorf("%w: deployment does not match plan", ErrRollingSchemaCheckpointInvalid)
	}
	for index, node := range nodes {
		if node.Node != plan.nodes[index] {
			return RollingSchemaCheckpoint{}, fmt.Errorf("%w: deployment node %q does not match plan", ErrRollingSchemaCheckpointInvalid, node.Node)
		}
	}
	checkpoint := RollingSchemaCheckpoint{
		PreviousFingerprint: plan.previousFingerprint,
		NextFingerprint:     plan.nextFingerprint,
		Nodes:               nodes,
	}
	if err := checkpoint.validate(); err != nil {
		return RollingSchemaCheckpoint{}, err
	}
	return checkpoint, nil
}

// Restore validates a checkpoint against this plan and returns a deployment
// that can resume idempotently. It never accepts a checkpoint for a different
// schema transition, node set, order, or phase.
func (plan RollingSchemaPlan) Restore(checkpoint RollingSchemaCheckpoint) (*RollingSchemaDeployment, error) {
	if err := checkpoint.validate(); err != nil {
		return nil, err
	}
	if len(plan.nodes) == 0 || checkpoint.PreviousFingerprint != plan.previousFingerprint || checkpoint.NextFingerprint != plan.nextFingerprint {
		return nil, fmt.Errorf("%w: checkpoint schema fingerprints do not match plan", ErrRollingSchemaCheckpointInvalid)
	}
	if len(checkpoint.Nodes) != len(plan.nodes) {
		return nil, fmt.Errorf("%w: checkpoint node count does not match plan", ErrRollingSchemaCheckpointInvalid)
	}
	deployment := plan.Begin()
	for index, node := range checkpoint.Nodes {
		if node.Node != plan.nodes[index] {
			return nil, fmt.Errorf("%w: checkpoint node %q does not match plan", ErrRollingSchemaCheckpointInvalid, node.Node)
		}
		deployment.phases[index] = node.Phase
	}
	return deployment, nil
}

func (checkpoint RollingSchemaCheckpoint) validate() error {
	if checkpoint.PreviousFingerprint == "" || checkpoint.NextFingerprint == "" {
		return fmt.Errorf("%w: schema fingerprints are required", ErrRollingSchemaCheckpointInvalid)
	}
	if len(checkpoint.PreviousFingerprint) > 256 || len(checkpoint.NextFingerprint) > 256 {
		return fmt.Errorf("%w: schema fingerprint is too long", ErrRollingSchemaCheckpointInvalid)
	}
	if len(checkpoint.Nodes) == 0 || len(checkpoint.Nodes) > MaxRollingSchemaCheckpointNodes {
		return fmt.Errorf("%w: node count must be between 1 and %d", ErrRollingSchemaCheckpointInvalid, MaxRollingSchemaCheckpointNodes)
	}
	previous := ""
	for index, node := range checkpoint.Nodes {
		if node.Node == "" || node.Node != trimASCIIWhitespace(node.Node) {
			return fmt.Errorf("%w: node %d is not normalized", ErrRollingSchemaCheckpointInvalid, index)
		}
		if len(node.Node) > 1<<16 {
			return fmt.Errorf("%w: node %q is too long", ErrRollingSchemaCheckpointInvalid, node.Node)
		}
		if index > 0 && previous >= node.Node {
			return fmt.Errorf("%w: nodes must be unique and sorted", ErrRollingSchemaCheckpointInvalid)
		}
		if node.Phase > RollingSchemaPhaseActive {
			return fmt.Errorf("%w: node %q has unsupported phase %d", ErrRollingSchemaCheckpointInvalid, node.Node, node.Phase)
		}
		previous = node.Node
	}
	return nil
}

// MarshalBinary encodes a bounded HRC1 checkpoint with canonical varint
// lengths and a Castagnoli CRC32 trailer. The encoding is deterministic.
func (checkpoint RollingSchemaCheckpoint) MarshalBinary() ([]byte, error) {
	if err := checkpoint.validate(); err != nil {
		return nil, err
	}
	capacity := len(rollingSchemaCheckpointMagic) + 1 + len(checkpoint.PreviousFingerprint) + len(checkpoint.NextFingerprint) + 16
	for _, node := range checkpoint.Nodes {
		capacity += len(node.Node) + 2
	}
	if capacity+4 > MaxRollingSchemaCheckpointBytes {
		return nil, fmt.Errorf("%w: encoded checkpoint exceeds %d bytes", ErrRollingSchemaCheckpointInvalid, MaxRollingSchemaCheckpointBytes)
	}
	frame := make([]byte, 0, capacity+4)
	frame = append(frame, rollingSchemaCheckpointMagic[:]...)
	frame = append(frame, rollingSchemaCheckpointVersion)
	frame = appendRollingSchemaCheckpointString(frame, checkpoint.PreviousFingerprint)
	frame = appendRollingSchemaCheckpointString(frame, checkpoint.NextFingerprint)
	frame = appendRollingSchemaCheckpointUvarint(frame, uint64(len(checkpoint.Nodes)))
	for _, node := range checkpoint.Nodes {
		frame = appendRollingSchemaCheckpointString(frame, node.Node)
		frame = append(frame, byte(node.Phase))
	}
	if len(frame)+4 > MaxRollingSchemaCheckpointBytes {
		return nil, fmt.Errorf("%w: encoded checkpoint exceeds %d bytes", ErrRollingSchemaCheckpointInvalid, MaxRollingSchemaCheckpointBytes)
	}
	checksum := crc32.Checksum(frame, rollingSchemaCheckpointCRCTable)
	var encoded [4]byte
	binary.BigEndian.PutUint32(encoded[:], checksum)
	frame = append(frame, encoded[:]...)
	return frame, nil
}

func appendRollingSchemaCheckpointString(dst []byte, value string) []byte {
	dst = appendRollingSchemaCheckpointUvarint(dst, uint64(len(value)))
	return append(dst, value...)
}

func appendRollingSchemaCheckpointUvarint(dst []byte, value uint64) []byte {
	var encoded [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(encoded[:], value)
	return append(dst, encoded[:n]...)
}

// UnmarshalBinary decodes one complete HRC1 checkpoint frame. The receiver is
// unchanged when decoding fails.
func (checkpoint *RollingSchemaCheckpoint) UnmarshalBinary(frame []byte) error {
	if checkpoint == nil {
		return ErrRollingSchemaCheckpointInvalid
	}
	if len(frame) < len(rollingSchemaCheckpointMagic)+1+4 || len(frame) > MaxRollingSchemaCheckpointBytes {
		return fmt.Errorf("%w: frame size is invalid", ErrRollingSchemaCheckpointInvalid)
	}
	payload := frame[:len(frame)-4]
	wantChecksum := binary.BigEndian.Uint32(frame[len(frame)-4:])
	if gotChecksum := crc32.Checksum(payload, rollingSchemaCheckpointCRCTable); gotChecksum != wantChecksum {
		return ErrRollingSchemaCheckpointChecksum
	}
	if !bytes.Equal(payload[:len(rollingSchemaCheckpointMagic)], rollingSchemaCheckpointMagic[:]) {
		return fmt.Errorf("%w: unknown frame magic", ErrRollingSchemaCheckpointInvalid)
	}
	position := len(rollingSchemaCheckpointMagic)
	if payload[position] != rollingSchemaCheckpointVersion {
		return fmt.Errorf("%w: unsupported frame version %d", ErrRollingSchemaCheckpointInvalid, payload[position])
	}
	position++
	previous, err := readRollingSchemaCheckpointString(payload, &position, 256)
	if err != nil {
		return err
	}
	next, err := readRollingSchemaCheckpointString(payload, &position, 256)
	if err != nil {
		return err
	}
	count, err := readRollingSchemaCheckpointUvarint(payload, &position)
	if err != nil {
		return err
	}
	if count == 0 || count > MaxRollingSchemaCheckpointNodes {
		return fmt.Errorf("%w: node count is invalid", ErrRollingSchemaCheckpointInvalid)
	}
	nodes := make([]RollingSchemaNode, int(count))
	for index := range nodes {
		node, err := readRollingSchemaCheckpointString(payload, &position, 1<<16)
		if err != nil {
			return err
		}
		if position >= len(payload) {
			return fmt.Errorf("%w: phase %d is truncated", ErrRollingSchemaCheckpointInvalid, index)
		}
		nodes[index] = RollingSchemaNode{Node: node, Phase: RollingSchemaPhase(payload[position])}
		position++
	}
	if position != len(payload) {
		return fmt.Errorf("%w: trailing payload bytes", ErrRollingSchemaCheckpointInvalid)
	}
	decoded := RollingSchemaCheckpoint{PreviousFingerprint: previous, NextFingerprint: next, Nodes: nodes}
	if err := decoded.validate(); err != nil {
		return err
	}
	*checkpoint = decoded
	return nil
}

func readRollingSchemaCheckpointUvarint(payload []byte, position *int) (uint64, error) {
	if position == nil || *position >= len(payload) {
		return 0, fmt.Errorf("%w: varint is truncated", ErrRollingSchemaCheckpointInvalid)
	}
	value, size := binary.Uvarint(payload[*position:])
	if size <= 0 {
		return 0, fmt.Errorf("%w: varint is invalid", ErrRollingSchemaCheckpointInvalid)
	}
	*position += size
	return value, nil
}

func readRollingSchemaCheckpointString(payload []byte, position *int, max int) (string, error) {
	length, err := readRollingSchemaCheckpointUvarint(payload, position)
	if err != nil {
		return "", err
	}
	if length > uint64(max) || length > uint64(len(payload)-*position) {
		return "", fmt.Errorf("%w: string length is invalid", ErrRollingSchemaCheckpointInvalid)
	}
	start := *position
	*position += int(length)
	return string(payload[start:*position]), nil
}

// DecodeRollingSchemaCheckpoint decodes one complete HRC1 checkpoint frame.
func DecodeRollingSchemaCheckpoint(frame []byte) (RollingSchemaCheckpoint, error) {
	var checkpoint RollingSchemaCheckpoint
	if err := checkpoint.UnmarshalBinary(frame); err != nil {
		return RollingSchemaCheckpoint{}, err
	}
	return checkpoint, nil
}

func trimASCIIWhitespace(value string) string {
	start, end := 0, len(value)
	for start < end && value[start] <= ' ' {
		start++
	}
	for end > start && value[end-1] <= ' ' {
		end--
	}
	return value[start:end]
}
