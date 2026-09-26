package hatCache

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"hatrie_cache/hat/hatReplication"
)

func TestTU047HTTPClusterWriteCommitPhases(t *testing.T) {
	participant, err := hatReplication.NewClusterWriteCommitParticipant(hatReplication.ClusterWriteCommitParticipantOptions{MaxRecords: 4})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(&ClusterWriteCommitHTTPHandler{
		Participant:      participant,
		ReplicationToken: "replication-secret",
	})
	defer server.Close()

	proposal := hatReplication.ClusterWriteCommitProposal{TransactionID: "tx-http", Sequence: 7, FenceToken: 9}
	proposal.PayloadDigest[0] = 0x42
	client := NewClusterWriteCommitHTTPClient(server.URL, server.Client(), "replication-secret")
	factoryCalls := 0
	result, err := ExecuteClusterWriteCommitOverHTTP(context.Background(), []string{server.URL}, proposal, func(context.Context, string) (*ClusterWriteCommitHTTPClient, error) {
		factoryCalls++
		return client, nil
	})
	if err != nil {
		t.Fatalf("ExecuteClusterWriteCommitOverHTTP() error = %v", err)
	}
	if !result.Committed || !result.Prepared || result.CommittedCount != 1 || factoryCalls != 1 {
		t.Fatalf("result = %#v, factory calls = %d", result, factoryCalls)
	}
	status, ok := participant.Status("tx-http")
	if !ok || status.Phase != hatReplication.ClusterWriteCommitParticipantCommitted {
		t.Fatalf("participant status = %#v, %t", status, ok)
	}
}

func TestTU047HTTPClusterWriteCommitStatusRoundTrip(t *testing.T) {
	participant, err := hatReplication.NewClusterWriteCommitParticipant(hatReplication.ClusterWriteCommitParticipantOptions{MaxRecords: 4})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(&ClusterWriteCommitHTTPHandler{
		Participant:      participant,
		ReplicationToken: "replication-secret",
	})
	defer server.Close()

	proposal := hatReplication.ClusterWriteCommitProposal{TransactionID: "tx-status", Sequence: 17, FenceToken: 23}
	proposal.PayloadDigest[0] = 0x7a
	if _, err := participant.Prepare(proposal); err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	client := NewClusterWriteCommitHTTPClient(server.URL, server.Client(), "replication-secret")
	got, found, err := client.Status(context.Background(), proposal.TransactionID)
	if err != nil {
		t.Fatalf("Status() error = %v", err)
	}
	if !found || got.Proposal != proposal || got.Phase != hatReplication.ClusterWriteCommitParticipantPrepared {
		t.Fatalf("Status() = %#v/%t, want prepared proposal", got, found)
	}
	if _, found, err := client.Status(context.Background(), "tx-missing"); err != nil || found {
		t.Fatalf("Status(missing) = %t/%v, want false/nil", found, err)
	}
}

func TestTU047HTTPClusterWriteCommitStatusRequiresAuthAndStrictQuery(t *testing.T) {
	participant, err := hatReplication.NewClusterWriteCommitParticipant(hatReplication.ClusterWriteCommitParticipantOptions{MaxRecords: 2})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(&ClusterWriteCommitHTTPHandler{
		Participant:      participant,
		ReplicationToken: "replication-secret",
	})
	defer server.Close()

	proposal := hatReplication.ClusterWriteCommitProposal{TransactionID: "tx-status-auth"}
	proposal.PayloadDigest[0] = 1
	if _, err := participant.Prepare(proposal); err != nil {
		t.Fatal(err)
	}
	if _, _, err := NewClusterWriteCommitHTTPClient(server.URL, server.Client(), "wrong-secret").Status(context.Background(), proposal.TransactionID); !errors.Is(err, ErrClusterWriteCommitHTTPUnauthorized) {
		t.Fatalf("wrong-token Status() error = %v, want unauthorized", err)
	}

	request := httptest.NewRequest(http.MethodGet, server.URL+"?transaction_id=tx-status-auth&extra=1", nil)
	request.Header.Set(clusterWriteCommitHTTPTokenHeader, "replication-secret")
	response := httptest.NewRecorder()
	(&ClusterWriteCommitHTTPHandler{Participant: participant, ReplicationToken: "replication-secret"}).ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("unknown status query field status = %d, want %d", response.Code, http.StatusBadRequest)
	}
}

func TestTU047HTTPClusterWriteCommitAuthAndValidation(t *testing.T) {
	participant, err := hatReplication.NewClusterWriteCommitParticipant(hatReplication.ClusterWriteCommitParticipantOptions{MaxRecords: 2})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(&ClusterWriteCommitHTTPHandler{
		Participant:      participant,
		ReplicationToken: "replication-secret",
	})
	defer server.Close()

	proposal := hatReplication.ClusterWriteCommitProposal{TransactionID: "tx-auth"}
	proposal.PayloadDigest[0] = 1
	if err := NewClusterWriteCommitHTTPClient(server.URL, server.Client(), "wrong-secret").Prepare(context.Background(), proposal); err == nil {
		t.Fatal("wrong token unexpectedly succeeded")
	}
	if _, ok := participant.Status("tx-auth"); ok {
		t.Fatal("unauthorized request mutated participant")
	}

	request := map[string]any{
		"transaction_id": "tx-invalid",
		"sequence":       1,
		"payload_digest": "AQ==",
		"phase":          "prepare",
	}
	payload, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	httpResponse, err := server.Client().Post(server.URL, "application/json", strings.NewReader(string(payload)))
	if err != nil {
		t.Fatal(err)
	}
	// The request is authenticated so validation reaches the digest check.
	// The client above intentionally used the wrong token only for the auth
	// assertion.
	httpResponse.Body.Close()
	requestWithToken, err := http.NewRequest(http.MethodPost, server.URL, strings.NewReader(string(payload)))
	if err != nil {
		t.Fatal(err)
	}
	requestWithToken.Header.Set("Content-Type", "application/json")
	requestWithToken.Header.Set(clusterWriteCommitHTTPTokenHeader, "replication-secret")
	httpResponse, err = server.Client().Do(requestWithToken)
	if err != nil {
		t.Fatal(err)
	}
	defer httpResponse.Body.Close()
	if httpResponse.StatusCode != http.StatusBadRequest {
		t.Fatalf("invalid digest status = %d, want %d", httpResponse.StatusCode, http.StatusBadRequest)
	}
}

func TestTU047HTTPClusterWriteCommitRejectsTrailingJSONAndMissingParticipant(t *testing.T) {
	server := httptest.NewServer(&ClusterWriteCommitHTTPHandler{})
	defer server.Close()
	proposal := hatReplication.ClusterWriteCommitProposal{TransactionID: "tx-missing"}
	proposal.PayloadDigest[0] = 1
	client := NewClusterWriteCommitHTTPClient(server.URL, server.Client(), "")
	if err := client.Prepare(context.Background(), proposal); !errors.Is(err, ErrClusterWriteCommitHTTPRemoteUnavailable) {
		t.Fatalf("missing participant error = %v", err)
	}

	participant, err := hatReplication.NewClusterWriteCommitParticipant(hatReplication.ClusterWriteCommitParticipantOptions{MaxRecords: 1})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, server.URL, strings.NewReader(`{"transaction_id":"tx-trailing","sequence":1,"payload_digest":"AQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyA=","phase":"prepare"}{}`))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	(&ClusterWriteCommitHTTPHandler{Participant: participant}).ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("trailing JSON status = %d, want %d", response.Code, http.StatusBadRequest)
	}
}

func TestTU047HTTPClusterWriteCommitBoundsMethodsAndBodies(t *testing.T) {
	participant, err := hatReplication.NewClusterWriteCommitParticipant(hatReplication.ClusterWriteCommitParticipantOptions{MaxRecords: 1})
	if err != nil {
		t.Fatal(err)
	}
	handler := &ClusterWriteCommitHTTPHandler{Participant: participant}

	getRequest := httptest.NewRequest(http.MethodGet, "http://example.test", nil)
	getResponse := httptest.NewRecorder()
	handler.ServeHTTP(getResponse, getRequest)
	if getResponse.Code != http.StatusBadRequest {
		t.Fatalf("GET without transaction status = %d, want %d", getResponse.Code, http.StatusBadRequest)
	}

	oversizedRequest := httptest.NewRequest(http.MethodPost, "http://example.test", strings.NewReader(strings.Repeat(" ", clusterWriteCommitHTTPMaxBodyBytes+1)))
	oversizedResponse := httptest.NewRecorder()
	handler.ServeHTTP(oversizedResponse, oversizedRequest)
	if oversizedResponse.Code != http.StatusBadRequest {
		t.Fatalf("oversized body status = %d, want %d", oversizedResponse.Code, http.StatusBadRequest)
	}
}
