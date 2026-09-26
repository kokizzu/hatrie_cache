package hatCache

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"hatrie_cache/hat/hatReplication"
)

const (
	clusterWriteCommitHTTPTokenHeader      = "X-Hatrie-Replication-Token"
	clusterWriteCommitHTTPMaxBodyBytes     = 64 << 10
	clusterWriteCommitHTTPMaxResponseBytes = 64 << 10
	clusterWriteCommitHTTPMaxMessageBytes  = 4 << 10
	clusterWriteCommitHTTPMaxQueryBytes    = 1 << 20
	clusterWriteCommitHTTPMaxTransactionID = 1 << 20
)

var (
	ErrClusterWriteCommitHTTPInvalidRequest    = errors.New("hatriecache: invalid cluster write commit HTTP request")
	ErrClusterWriteCommitHTTPUnauthorized      = errors.New("hatriecache: unauthorized cluster write commit HTTP request")
	ErrClusterWriteCommitHTTPRemoteUnavailable = errors.New("hatriecache: cluster write commit HTTP remote unavailable")
	ErrClusterWriteCommitHTTPResponseInvalid   = errors.New("hatriecache: invalid cluster write commit HTTP response")
	ErrClusterWriteCommitHTTPPhaseRejected     = errors.New("hatriecache: cluster write commit HTTP phase rejected")
	ErrClusterWriteCommitHTTPStatusInvalid     = errors.New("hatriecache: invalid cluster write commit HTTP status")
)

// ClusterWriteCommitHTTPRequest is the bounded JSON wire request for one
// prepare, commit, or abort phase. PayloadDigest is encoded by encoding/json
// as base64, matching the standard JSON representation for []byte.
type ClusterWriteCommitHTTPRequest struct {
	TransactionID string `json:"transaction_id"`
	Sequence      uint64 `json:"sequence"`
	FenceToken    uint64 `json:"fence_token"`
	PayloadDigest []byte `json:"payload_digest"`
	Phase         string `json:"phase"`
}

// ClusterWriteCommitHTTPResponse is returned for both successful and
// participant-rejected phase calls. HTTP status codes are reserved for
// transport, authentication, and request validation failures.
type ClusterWriteCommitHTTPResponse struct {
	OK            bool   `json:"ok"`
	Found         bool   `json:"found"`
	Message       string `json:"message,omitempty"`
	Phase         string `json:"phase,omitempty"`
	TransactionID string `json:"transaction_id,omitempty"`
	Sequence      uint64 `json:"sequence,omitempty"`
	FenceToken    uint64 `json:"fence_token,omitempty"`
	PayloadDigest []byte `json:"payload_digest,omitempty"`
}

// ClusterWriteCommitHTTPHandler exposes one participant as an opt-in HTTP
// handler. Mount it on a caller-owned route and server; the default cache
// HTTP surface does not register this handler automatically.
type ClusterWriteCommitHTTPHandler struct {
	Participant      *hatReplication.ClusterWriteCommitParticipant
	ReplicationToken string
}

// ClusterWriteCommitHTTPClient sends phase calls to one caller-owned HTTP
// endpoint. The client and connection pooling policy remain caller-owned.
type ClusterWriteCommitHTTPClient struct {
	endpoint         string
	httpClient       *http.Client
	replicationToken string
}

// ClusterWriteCommitHTTPClientFactory creates one reusable HTTP phase client
// for a participant node. The caller owns the HTTP client and its transport.
type ClusterWriteCommitHTTPClientFactory func(context.Context, string) (*ClusterWriteCommitHTTPClient, error)

// NewClusterWriteCommitHTTPClient creates a phase client. Endpoint validation
// is repeated at call time so a zero-value or malformed configuration cannot
// accidentally issue a request.
func NewClusterWriteCommitHTTPClient(endpoint string, httpClient *http.Client, replicationToken string) *ClusterWriteCommitHTTPClient {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	return &ClusterWriteCommitHTTPClient{
		endpoint:         strings.TrimSpace(endpoint),
		httpClient:       httpClient,
		replicationToken: strings.TrimSpace(replicationToken),
	}
}

// ExecuteClusterWriteCommitOverHTTP binds the existing transport-neutral
// coordinator to one reusable HTTP client per participant. It is opt-in and
// does not change the asynchronous replication path.
func ExecuteClusterWriteCommitOverHTTP(
	ctx context.Context,
	nodes []string,
	proposal hatReplication.ClusterWriteCommitProposal,
	factory ClusterWriteCommitHTTPClientFactory,
) (hatReplication.ClusterWriteCommitResult, error) {
	if ctx == nil || factory == nil {
		return hatReplication.ClusterWriteCommitResult{Proposal: proposal}, hatReplication.ErrClusterWriteCommitInvalid
	}
	if len(nodes) == 0 || len(nodes) > hatReplication.MaxClusterWriteCommitNodes {
		return hatReplication.ClusterWriteCommitResult{Proposal: proposal}, hatReplication.ErrClusterWriteCommitInvalid
	}
	normalizedNodes := make([]string, len(nodes))
	clients := make(map[string]*ClusterWriteCommitHTTPClient, len(nodes))
	for index, node := range nodes {
		node = strings.TrimSpace(node)
		if node == "" {
			return hatReplication.ClusterWriteCommitResult{Proposal: proposal}, hatReplication.ErrClusterWriteCommitInvalid
		}
		if _, exists := clients[node]; exists {
			return hatReplication.ClusterWriteCommitResult{Proposal: proposal}, hatReplication.ErrClusterWriteCommitInvalid
		}
		client, err := factory(ctx, node)
		if err != nil {
			return hatReplication.ClusterWriteCommitResult{Proposal: proposal}, err
		}
		if client == nil {
			return hatReplication.ClusterWriteCommitResult{Proposal: proposal}, fmt.Errorf("hatriecache: nil HTTP cluster write commit client for %q", node)
		}
		normalizedNodes[index] = node
		clients[node] = client
	}
	lookup := func(node string) (*ClusterWriteCommitHTTPClient, error) {
		client := clients[node]
		if client == nil {
			return nil, fmt.Errorf("hatriecache: HTTP cluster write commit client for %q is not configured", node)
		}
		return client, nil
	}
	return hatReplication.ExecuteClusterWriteCommit(
		ctx,
		normalizedNodes,
		proposal,
		func(ctx context.Context, node string, proposal hatReplication.ClusterWriteCommitProposal) error {
			client, err := lookup(node)
			if err != nil {
				return err
			}
			return client.Prepare(ctx, proposal)
		},
		func(ctx context.Context, node string, proposal hatReplication.ClusterWriteCommitProposal) error {
			client, err := lookup(node)
			if err != nil {
				return err
			}
			return client.Commit(ctx, proposal)
		},
		func(ctx context.Context, node string, proposal hatReplication.ClusterWriteCommitProposal) error {
			client, err := lookup(node)
			if err != nil {
				return err
			}
			return client.Abort(ctx, proposal)
		},
	)
}

// Prepare records a proposal on the remote participant without making it
// visible.
func (client *ClusterWriteCommitHTTPClient) Prepare(ctx context.Context, proposal hatReplication.ClusterWriteCommitProposal) error {
	return client.call(ctx, "prepare", proposal)
}

// Commit makes a previously prepared proposal visible on the remote
// participant.
func (client *ClusterWriteCommitHTTPClient) Commit(ctx context.Context, proposal hatReplication.ClusterWriteCommitProposal) error {
	return client.call(ctx, "commit", proposal)
}

// Abort releases a previously prepared proposal on the remote participant.
func (client *ClusterWriteCommitHTTPClient) Abort(ctx context.Context, proposal hatReplication.ClusterWriteCommitProposal) error {
	return client.call(ctx, "abort", proposal)
}

// Status reads one participant transaction without changing participant state.
// It is intended for recovery after a coordinator reports an indeterminate
// commit outcome.
func (client *ClusterWriteCommitHTTPClient) Status(ctx context.Context, transactionID string) (hatReplication.ClusterWriteCommitParticipantRecord, bool, error) {
	if client == nil || !validClusterWriteCommitHTTPEndpoint(client.endpoint) {
		return hatReplication.ClusterWriteCommitParticipantRecord{}, false, ErrClusterWriteCommitHTTPRemoteUnavailable
	}
	transactionID = strings.TrimSpace(transactionID)
	if len(transactionID) == 0 || len(transactionID) > clusterWriteCommitHTTPMaxTransactionID {
		return hatReplication.ClusterWriteCommitParticipantRecord{}, false, ErrClusterWriteCommitHTTPStatusInvalid
	}
	if ctx == nil {
		ctx = context.Background()
	}
	statusEndpoint, err := clusterWriteCommitHTTPStatusEndpoint(client.endpoint, transactionID)
	if err != nil {
		return hatReplication.ClusterWriteCommitParticipantRecord{}, false, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, statusEndpoint, nil)
	if err != nil {
		return hatReplication.ClusterWriteCommitParticipantRecord{}, false, fmt.Errorf("%w: create request: %v", ErrClusterWriteCommitHTTPRemoteUnavailable, err)
	}
	if client.replicationToken != "" {
		request.Header.Set(clusterWriteCommitHTTPTokenHeader, client.replicationToken)
	}
	response, err := client.httpClient.Do(request)
	if err != nil {
		return hatReplication.ClusterWriteCommitParticipantRecord{}, false, fmt.Errorf("%w: %v", ErrClusterWriteCommitHTTPRemoteUnavailable, err)
	}
	defer response.Body.Close()
	decoded, err := decodeClusterWriteCommitHTTPResponse(response.Body)
	if err != nil {
		if response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusForbidden {
			return hatReplication.ClusterWriteCommitParticipantRecord{}, false, ErrClusterWriteCommitHTTPUnauthorized
		}
		return hatReplication.ClusterWriteCommitParticipantRecord{}, false, err
	}
	if response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusForbidden {
		return hatReplication.ClusterWriteCommitParticipantRecord{}, false, ErrClusterWriteCommitHTTPUnauthorized
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return hatReplication.ClusterWriteCommitParticipantRecord{}, false, ErrClusterWriteCommitHTTPRemoteUnavailable
	}
	if !decoded.OK {
		if decoded.Message != "" {
			return hatReplication.ClusterWriteCommitParticipantRecord{}, false, fmt.Errorf("%w: %s", ErrClusterWriteCommitHTTPStatusInvalid, decoded.Message)
		}
		return hatReplication.ClusterWriteCommitParticipantRecord{}, false, ErrClusterWriteCommitHTTPStatusInvalid
	}
	if !decoded.Found {
		return hatReplication.ClusterWriteCommitParticipantRecord{}, false, nil
	}
	phase, ok := normalizeClusterWriteCommitHTTPParticipantPhase(decoded.Phase)
	if !ok || decoded.TransactionID != transactionID || len(decoded.PayloadDigest) != clusterWriteCommitPayloadDigestSize {
		return hatReplication.ClusterWriteCommitParticipantRecord{}, false, ErrClusterWriteCommitHTTPStatusInvalid
	}
	var digest [clusterWriteCommitPayloadDigestSize]byte
	copy(digest[:], decoded.PayloadDigest)
	return hatReplication.ClusterWriteCommitParticipantRecord{
		Proposal: hatReplication.ClusterWriteCommitProposal{
			TransactionID: decoded.TransactionID,
			Sequence:      decoded.Sequence,
			FenceToken:    decoded.FenceToken,
			PayloadDigest: digest,
		},
		Phase: phase,
	}, true, nil
}

func clusterWriteCommitHTTPStatusEndpoint(endpoint, transactionID string) (string, error) {
	parsed, err := url.Parse(endpoint)
	if err != nil || !parsed.IsAbs() || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return "", ErrClusterWriteCommitHTTPRemoteUnavailable
	}
	if len(parsed.RawQuery) > clusterWriteCommitHTTPMaxQueryBytes {
		return "", ErrClusterWriteCommitHTTPStatusInvalid
	}
	query, err := url.ParseQuery(parsed.RawQuery)
	if err != nil {
		return "", ErrClusterWriteCommitHTTPStatusInvalid
	}
	query.Set("transaction_id", transactionID)
	parsed.RawQuery = query.Encode()
	if len(parsed.RawQuery) > clusterWriteCommitHTTPMaxQueryBytes {
		return "", ErrClusterWriteCommitHTTPStatusInvalid
	}
	return parsed.String(), nil
}

func (client *ClusterWriteCommitHTTPClient) call(ctx context.Context, phase string, proposal hatReplication.ClusterWriteCommitProposal) error {
	if client == nil || !validClusterWriteCommitHTTPEndpoint(client.endpoint) {
		return ErrClusterWriteCommitHTTPRemoteUnavailable
	}
	if ctx == nil {
		ctx = context.Background()
	}
	payload, err := json.Marshal(ClusterWriteCommitHTTPRequest{
		TransactionID: proposal.TransactionID,
		Sequence:      proposal.Sequence,
		FenceToken:    proposal.FenceToken,
		PayloadDigest: append([]byte(nil), proposal.PayloadDigest[:]...),
		Phase:         phase,
	})
	if err != nil {
		return fmt.Errorf("%w: encode request: %v", ErrClusterWriteCommitHTTPRemoteUnavailable, err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, client.endpoint, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("%w: create request: %v", ErrClusterWriteCommitHTTPRemoteUnavailable, err)
	}
	request.Header.Set("Content-Type", "application/json")
	if client.replicationToken != "" {
		request.Header.Set(clusterWriteCommitHTTPTokenHeader, client.replicationToken)
	}
	response, err := client.httpClient.Do(request)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrClusterWriteCommitHTTPRemoteUnavailable, err)
	}
	defer response.Body.Close()
	decoded, err := decodeClusterWriteCommitHTTPResponse(response.Body)
	if err != nil {
		if response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusForbidden {
			return ErrClusterWriteCommitHTTPUnauthorized
		}
		return err
	}
	if response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusForbidden {
		return ErrClusterWriteCommitHTTPUnauthorized
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		if decoded.Message != "" {
			return fmt.Errorf("%w: %s", ErrClusterWriteCommitHTTPRemoteUnavailable, decoded.Message)
		}
		return ErrClusterWriteCommitHTTPRemoteUnavailable
	}
	if decoded.Phase != phase {
		return fmt.Errorf("%w: response phase %q does not match %q", ErrClusterWriteCommitHTTPResponseInvalid, decoded.Phase, phase)
	}
	if !decoded.OK {
		if decoded.Message == "" {
			return ErrClusterWriteCommitHTTPPhaseRejected
		}
		return fmt.Errorf("%w: %s", ErrClusterWriteCommitHTTPPhaseRejected, decoded.Message)
	}
	return nil
}

// ServeHTTP handles one phase request or one authenticated status read. It
// does not expose the participant through the default monitoring router.
func (handler *ClusterWriteCommitHTTPHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if writer == nil {
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	if request == nil {
		writeClusterWriteCommitHTTPResponse(writer, http.StatusBadRequest, ClusterWriteCommitHTTPResponse{Message: ErrClusterWriteCommitHTTPInvalidRequest.Error()})
		return
	}
	if request.Method != http.MethodGet && request.Method != http.MethodPost {
		writer.Header().Set("Allow", http.MethodGet+", "+http.MethodPost)
		writeClusterWriteCommitHTTPResponse(writer, http.StatusMethodNotAllowed, ClusterWriteCommitHTTPResponse{Message: "GET or POST is required"})
		return
	}
	expectedToken := ""
	if handler != nil {
		expectedToken = strings.TrimSpace(handler.ReplicationToken)
	}
	if expectedToken != "" && subtle.ConstantTimeCompare([]byte(expectedToken), []byte(request.Header.Get(clusterWriteCommitHTTPTokenHeader))) != 1 {
		writeClusterWriteCommitHTTPResponse(writer, http.StatusUnauthorized, ClusterWriteCommitHTTPResponse{Message: ErrClusterWriteCommitHTTPUnauthorized.Error()})
		return
	}
	if handler == nil || handler.Participant == nil {
		writeClusterWriteCommitHTTPResponse(writer, http.StatusServiceUnavailable, ClusterWriteCommitHTTPResponse{Message: ErrClusterWriteCommitHTTPRemoteUnavailable.Error()})
		return
	}
	if err := request.Context().Err(); err != nil {
		return
	}
	if request.Method == http.MethodGet {
		handler.serveStatus(writer, request)
		return
	}
	decoded, err := decodeClusterWriteCommitHTTPRequest(request.Body)
	if err != nil {
		writeClusterWriteCommitHTTPResponse(writer, http.StatusBadRequest, ClusterWriteCommitHTTPResponse{Message: err.Error()})
		return
	}
	phase, ok := normalizeClusterWriteCommitHTTPPhase(decoded.Phase)
	if !ok || len(decoded.PayloadDigest) != clusterWriteCommitPayloadDigestSize {
		writeClusterWriteCommitHTTPResponse(writer, http.StatusBadRequest, ClusterWriteCommitHTTPResponse{Message: ErrClusterWriteCommitHTTPInvalidRequest.Error()})
		return
	}
	proposal := hatReplication.ClusterWriteCommitProposal{
		TransactionID: strings.TrimSpace(decoded.TransactionID),
		Sequence:      decoded.Sequence,
		FenceToken:    decoded.FenceToken,
	}
	copy(proposal.PayloadDigest[:], decoded.PayloadDigest)
	response := ClusterWriteCommitHTTPResponse{Phase: phase}
	switch phase {
	case "prepare":
		_, err = handler.Participant.Prepare(proposal)
	case "commit":
		_, err = handler.Participant.Commit(proposal)
	case "abort":
		_, err = handler.Participant.Abort(proposal)
	}
	if err != nil {
		response.Message = boundedClusterWriteCommitHTTPMessage(err.Error())
		writeClusterWriteCommitHTTPResponse(writer, http.StatusOK, response)
		return
	}
	response.OK = true
	writeClusterWriteCommitHTTPResponse(writer, http.StatusOK, response)
}

func (handler *ClusterWriteCommitHTTPHandler) serveStatus(writer http.ResponseWriter, request *http.Request) {
	if len(request.URL.RawQuery) > clusterWriteCommitHTTPMaxQueryBytes {
		writeClusterWriteCommitHTTPResponse(writer, http.StatusBadRequest, ClusterWriteCommitHTTPResponse{Message: ErrClusterWriteCommitHTTPInvalidRequest.Error()})
		return
	}
	query, err := url.ParseQuery(request.URL.RawQuery)
	if err != nil || len(query) != 1 || len(query["transaction_id"]) != 1 {
		writeClusterWriteCommitHTTPResponse(writer, http.StatusBadRequest, ClusterWriteCommitHTTPResponse{Message: ErrClusterWriteCommitHTTPInvalidRequest.Error()})
		return
	}
	transactionID := strings.TrimSpace(query.Get("transaction_id"))
	if len(transactionID) == 0 || len(transactionID) > clusterWriteCommitHTTPMaxTransactionID {
		writeClusterWriteCommitHTTPResponse(writer, http.StatusBadRequest, ClusterWriteCommitHTTPResponse{Message: ErrClusterWriteCommitHTTPInvalidRequest.Error()})
		return
	}
	record, found := handler.Participant.Status(transactionID)
	response := ClusterWriteCommitHTTPResponse{OK: true, Found: found}
	if found {
		phase, ok := clusterWriteCommitHTTPParticipantPhaseName(record.Phase)
		if !ok {
			writeClusterWriteCommitHTTPResponse(writer, http.StatusServiceUnavailable, ClusterWriteCommitHTTPResponse{Message: ErrClusterWriteCommitHTTPStatusInvalid.Error()})
			return
		}
		response.Phase = phase
		response.TransactionID = record.Proposal.TransactionID
		response.Sequence = record.Proposal.Sequence
		response.FenceToken = record.Proposal.FenceToken
		response.PayloadDigest = append([]byte(nil), record.Proposal.PayloadDigest[:]...)
	}
	writeClusterWriteCommitHTTPResponse(writer, http.StatusOK, response)
}

func decodeClusterWriteCommitHTTPRequest(reader io.Reader) (ClusterWriteCommitHTTPRequest, error) {
	if reader == nil {
		return ClusterWriteCommitHTTPRequest{}, ErrClusterWriteCommitHTTPInvalidRequest
	}
	payload, err := io.ReadAll(io.LimitReader(reader, clusterWriteCommitHTTPMaxBodyBytes+1))
	if err != nil {
		return ClusterWriteCommitHTTPRequest{}, fmt.Errorf("%w: read body: %v", ErrClusterWriteCommitHTTPInvalidRequest, err)
	}
	if len(payload) > clusterWriteCommitHTTPMaxBodyBytes {
		return ClusterWriteCommitHTTPRequest{}, ErrClusterWriteCommitHTTPInvalidRequest
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var request ClusterWriteCommitHTTPRequest
	if err := decoder.Decode(&request); err != nil {
		return ClusterWriteCommitHTTPRequest{}, fmt.Errorf("%w: %v", ErrClusterWriteCommitHTTPInvalidRequest, err)
	}
	if err := ensureClusterWriteCommitHTTPEOF(decoder); err != nil {
		return ClusterWriteCommitHTTPRequest{}, err
	}
	if len(request.TransactionID) == 0 || len(request.TransactionID) > clusterWriteCommitHTTPMaxTransactionID {
		return ClusterWriteCommitHTTPRequest{}, ErrClusterWriteCommitHTTPInvalidRequest
	}
	if len(request.PayloadDigest) != clusterWriteCommitPayloadDigestSize {
		return ClusterWriteCommitHTTPRequest{}, ErrClusterWriteCommitHTTPInvalidRequest
	}
	return request, nil
}

func decodeClusterWriteCommitHTTPResponse(reader io.Reader) (ClusterWriteCommitHTTPResponse, error) {
	if reader == nil {
		return ClusterWriteCommitHTTPResponse{}, ErrClusterWriteCommitHTTPResponseInvalid
	}
	payload, err := io.ReadAll(io.LimitReader(reader, clusterWriteCommitHTTPMaxResponseBytes+1))
	if err != nil {
		return ClusterWriteCommitHTTPResponse{}, fmt.Errorf("%w: read body: %v", ErrClusterWriteCommitHTTPResponseInvalid, err)
	}
	if len(payload) > clusterWriteCommitHTTPMaxResponseBytes {
		return ClusterWriteCommitHTTPResponse{}, ErrClusterWriteCommitHTTPResponseInvalid
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var response ClusterWriteCommitHTTPResponse
	if err := decoder.Decode(&response); err != nil {
		return ClusterWriteCommitHTTPResponse{}, fmt.Errorf("%w: %v", ErrClusterWriteCommitHTTPResponseInvalid, err)
	}
	if err := ensureClusterWriteCommitHTTPEOF(decoder); err != nil {
		return ClusterWriteCommitHTTPResponse{}, err
	}
	response.Message = boundedClusterWriteCommitHTTPMessage(response.Message)
	return response, nil
}

func ensureClusterWriteCommitHTTPEOF(decoder *json.Decoder) error {
	var trailing interface{}
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return ErrClusterWriteCommitHTTPInvalidRequest
		}
		return fmt.Errorf("%w: trailing JSON: %v", ErrClusterWriteCommitHTTPInvalidRequest, err)
	}
	return nil
}

func normalizeClusterWriteCommitHTTPPhase(phase string) (string, bool) {
	switch strings.ToLower(strings.TrimSpace(phase)) {
	case "prepare":
		return "prepare", true
	case "commit":
		return "commit", true
	case "abort":
		return "abort", true
	default:
		return "", false
	}
}

func normalizeClusterWriteCommitHTTPParticipantPhase(phase string) (hatReplication.ClusterWriteCommitParticipantPhase, bool) {
	switch strings.ToLower(strings.TrimSpace(phase)) {
	case "prepared":
		return hatReplication.ClusterWriteCommitParticipantPrepared, true
	case "committed":
		return hatReplication.ClusterWriteCommitParticipantCommitted, true
	case "aborted":
		return hatReplication.ClusterWriteCommitParticipantAborted, true
	default:
		return 0, false
	}
}

func clusterWriteCommitHTTPParticipantPhaseName(phase hatReplication.ClusterWriteCommitParticipantPhase) (string, bool) {
	switch phase {
	case hatReplication.ClusterWriteCommitParticipantPrepared:
		return "prepared", true
	case hatReplication.ClusterWriteCommitParticipantCommitted:
		return "committed", true
	case hatReplication.ClusterWriteCommitParticipantAborted:
		return "aborted", true
	default:
		return "", false
	}
}

func validClusterWriteCommitHTTPEndpoint(endpoint string) bool {
	parsed, err := url.Parse(endpoint)
	return err == nil && parsed.IsAbs() && parsed.Host != "" && (parsed.Scheme == "http" || parsed.Scheme == "https")
}

func boundedClusterWriteCommitHTTPMessage(message string) string {
	message = strings.TrimSpace(message)
	if len(message) > clusterWriteCommitHTTPMaxMessageBytes {
		return message[:clusterWriteCommitHTTPMaxMessageBytes]
	}
	return message
}

func writeClusterWriteCommitHTTPResponse(writer http.ResponseWriter, status int, response ClusterWriteCommitHTTPResponse) {
	writer.WriteHeader(status)
	encoder := json.NewEncoder(writer)
	_ = encoder.Encode(response)
}
