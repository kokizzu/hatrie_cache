package hatriecache

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	hatriecachev1 "hatrie_cache/internal/gen/hatriecache/v1"
)

const testGRPCBufferSize = 1024 * 1024

type pipeliningReplicationGRPCServer struct {
	hatriecachev1.UnimplementedCacheServiceServer
	pipelined chan struct{}
	release   chan struct{}
}

func (server *pipeliningReplicationGRPCServer) ReplicationStream(stream hatriecachev1.CacheService_ReplicationStreamServer) error {
	warmup, err := stream.Recv()
	if err != nil {
		return err
	}
	if err := stream.Send(&hatriecachev1.ReplicationStreamAck{Sequence: warmup.GetSequence(), Ok: true, Entries: uint64(len(warmup.GetKeys()))}); err != nil {
		return err
	}
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	second, err := stream.Recv()
	if err != nil {
		return err
	}
	close(server.pipelined)
	select {
	case <-server.release:
	case <-stream.Context().Done():
		return stream.Context().Err()
	}
	for _, batch := range []*hatriecachev1.ReplicationStreamBatch{first, second} {
		if err := stream.Send(&hatriecachev1.ReplicationStreamAck{Sequence: batch.GetSequence(), Ok: true, Entries: uint64(len(batch.GetKeys()))}); err != nil {
			return err
		}
	}
	for {
		batch, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := stream.Send(&hatriecachev1.ReplicationStreamAck{Sequence: batch.GetSequence(), Ok: true, Entries: uint64(len(batch.GetKeys()))}); err != nil {
			return err
		}
	}
}

func TestCacheGRPCServerHealthStatsEntriesAndCommands(t *testing.T) {
	ht := newTestTrie(t)
	now := time.Unix(2000, 0)
	ht.now = func() time.Time { return now }
	ht.UpsertString("session:1", "active user")
	if !ht.Expire("session:1", time.Minute) {
		t.Fatal("Expire(session:1) = false, want true")
	}

	client, stop := newTestGRPCClient(t, ht, CacheGRPCOptions{
		NodeName: "test-node",
		StartAt:  time.Now().Add(-time.Hour),
	})
	defer stop()

	health, err := client.Health(context.Background(), &hatriecachev1.HealthRequest{})
	if err != nil {
		t.Fatalf("Health() error = %v", err)
	}
	if health.GetStatus() != "online" || health.GetNode() != "test-node" {
		t.Fatalf("Health() = %#v, want online test-node", health)
	}

	setResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "SETSTR",
		Key:     "name",
		Value:   "ivi",
	})
	if err != nil {
		t.Fatalf("Command(SETSTR) error = %v", err)
	}
	if !setResp.GetOk() {
		t.Fatalf("SETSTR response = %#v, want ok", setResp)
	}

	ttlSeconds := int64(30)
	setIntResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command:    "SETINT",
		Key:        "views",
		Value:      "41",
		TtlSeconds: &ttlSeconds,
	})
	if err != nil {
		t.Fatalf("Command(SETINT) error = %v", err)
	}
	if !setIntResp.GetOk() {
		t.Fatalf("SETINT response = %#v, want ok", setIntResp)
	}
	incResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "INC",
		Key:     "views",
	})
	if err != nil {
		t.Fatalf("Command(INC) error = %v", err)
	}
	if !incResp.GetOk() || incResp.GetValue() != "42" {
		t.Fatalf("INC response = %#v, want 42", incResp)
	}

	putMapResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "PUTMAP",
		Key:     "profile",
		Pairs:   map[string]string{"city": "Singapore"},
	})
	if err != nil {
		t.Fatalf("Command(PUTMAP) error = %v", err)
	}
	if !putMapResp.GetOk() {
		t.Fatalf("PUTMAP response = %#v, want ok", putMapResp)
	}
	peekMapResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "PEEKMAP",
		Key:     "profile",
		Subkey:  "city",
	})
	if err != nil {
		t.Fatalf("Command(PEEKMAP) error = %v", err)
	}
	if !peekMapResp.GetOk() || peekMapResp.GetValue() != "Singapore" {
		t.Fatalf("PEEKMAP response = %#v, want Singapore", peekMapResp)
	}

	pushResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "PUSHSLICE",
		Key:     "jobs",
		Values:  []string{"build", "verify"},
	})
	if err != nil {
		t.Fatalf("Command(PUSHSLICE) error = %v", err)
	}
	if !pushResp.GetOk() {
		t.Fatalf("PUSHSLICE response = %#v, want ok", pushResp)
	}
	tailResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "TAILSLICE",
		Key:     "jobs",
	})
	if err != nil {
		t.Fatalf("Command(TAILSLICE) error = %v", err)
	}
	if !tailResp.GetOk() || tailResp.GetValue() != "verify" {
		t.Fatalf("TAILSLICE response = %#v, want verify", tailResp)
	}

	priority := int64(1)
	pushPQResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command:  "PUSHPQ",
		Key:      "priority:jobs",
		Value:    "urgent",
		Priority: &priority,
	})
	if err != nil {
		t.Fatalf("Command(PUSHPQ) error = %v", err)
	}
	if !pushPQResp.GetOk() {
		t.Fatalf("PUSHPQ response = %#v, want ok", pushPQResp)
	}
	popPQResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "POPPQ",
		Key:     "priority:jobs",
	})
	if err != nil {
		t.Fatalf("Command(POPPQ) error = %v", err)
	}
	if !popPQResp.GetOk() || popPQResp.GetValue() != `{"priority":1,"value":"urgent"}` {
		t.Fatalf("POPPQ response = %#v, want urgent priority item", popPQResp)
	}

	createBloomResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "CREATEBF",
		Key:     "seen",
		Value:   "1000",
		Subkey:  "0.001",
	})
	if err != nil {
		t.Fatalf("Command(CREATEBF) error = %v", err)
	}
	if !createBloomResp.GetOk() {
		t.Fatalf("CREATEBF response = %#v, want ok", createBloomResp)
	}
	addBloomResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "ADDBF",
		Key:     "seen",
		Values:  []string{"alpha", "beta"},
	})
	if err != nil {
		t.Fatalf("Command(ADDBF) error = %v", err)
	}
	if !addBloomResp.GetOk() || addBloomResp.GetValue() != "2" {
		t.Fatalf("ADDBF response = %#v, want added 2", addBloomResp)
	}
	hasBloomResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "HASBF",
		Key:     "seen",
		Value:   "alpha",
	})
	if err != nil {
		t.Fatalf("Command(HASBF) error = %v", err)
	}
	if !hasBloomResp.GetOk() || hasBloomResp.GetValue() != "1" {
		t.Fatalf("HASBF response = %#v, want hit", hasBloomResp)
	}
	infoBloomResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "INFOBF",
		Key:     "seen",
	})
	if err != nil {
		t.Fatalf("Command(INFOBF) error = %v", err)
	}
	if !infoBloomResp.GetOk() || infoBloomResp.GetValue() == "" {
		t.Fatalf("INFOBF response = %#v, want JSON info", infoBloomResp)
	}

	createXorResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "CREATEXF",
		Key:     "xor:seen",
		Value:   "8",
	})
	if err != nil {
		t.Fatalf("Command(CREATEXF) error = %v", err)
	}
	if !createXorResp.GetOk() {
		t.Fatalf("CREATEXF response = %#v, want ok", createXorResp)
	}
	addXorResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "ADDXF",
		Key:     "xor:seen",
		Values:  []string{"alpha", "beta"},
	})
	if err != nil {
		t.Fatalf("Command(ADDXF) error = %v", err)
	}
	if !addXorResp.GetOk() || addXorResp.GetValue() != "2" {
		t.Fatalf("ADDXF response = %#v, want staged 2", addXorResp)
	}
	buildXorResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "BUILDXF",
		Key:     "xor:seen",
	})
	if err != nil {
		t.Fatalf("Command(BUILDXF) error = %v", err)
	}
	if !buildXorResp.GetOk() || buildXorResp.GetValue() == "" {
		t.Fatalf("BUILDXF response = %#v, want JSON info", buildXorResp)
	}
	hasXorResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "HASXF",
		Key:     "xor:seen",
		Value:   "alpha",
	})
	if err != nil {
		t.Fatalf("Command(HASXF) error = %v", err)
	}
	if !hasXorResp.GetOk() || hasXorResp.GetValue() != "1" {
		t.Fatalf("HASXF response = %#v, want hit", hasXorResp)
	}

	createCuckooResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "CREATECF",
		Key:     "active",
		Value:   "128",
		Subkey:  "0.001",
	})
	if err != nil {
		t.Fatalf("Command(CREATECF) error = %v", err)
	}
	if !createCuckooResp.GetOk() {
		t.Fatalf("CREATECF response = %#v, want ok", createCuckooResp)
	}
	addCuckooResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "ADDCF",
		Key:     "active",
		Values:  []string{"alpha", "beta"},
	})
	if err != nil {
		t.Fatalf("Command(ADDCF) error = %v", err)
	}
	if !addCuckooResp.GetOk() || addCuckooResp.GetValue() != "2" {
		t.Fatalf("ADDCF response = %#v, want added 2", addCuckooResp)
	}
	hasCuckooResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "HASCF",
		Key:     "active",
		Value:   "alpha",
	})
	if err != nil {
		t.Fatalf("Command(HASCF) error = %v", err)
	}
	if !hasCuckooResp.GetOk() || hasCuckooResp.GetValue() != "1" {
		t.Fatalf("HASCF response = %#v, want hit", hasCuckooResp)
	}
	delCuckooResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "DELCF",
		Key:     "active",
		Value:   "alpha",
	})
	if err != nil {
		t.Fatalf("Command(DELCF) error = %v", err)
	}
	if !delCuckooResp.GetOk() || delCuckooResp.GetValue() != "1" {
		t.Fatalf("DELCF response = %#v, want removed 1", delCuckooResp)
	}
	infoCuckooResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "INFOCF",
		Key:     "active",
	})
	if err != nil {
		t.Fatalf("Command(INFOCF) error = %v", err)
	}
	if !infoCuckooResp.GetOk() || infoCuckooResp.GetValue() == "" {
		t.Fatalf("INFOCF response = %#v, want JSON info", infoCuckooResp)
	}
	var cuckooInfo CuckooFilterInfo
	if err := json.Unmarshal([]byte(infoCuckooResp.GetValue()), &cuckooInfo); err != nil {
		t.Fatalf("INFOCF JSON error = %v", err)
	}
	if cuckooInfo.Count != 1 || cuckooInfo.BucketSize != cuckooFilterBucketSize || cuckooInfo.FingerprintBytes == 0 {
		t.Fatalf("INFOCF = %#v, want one compact filter value", cuckooInfo)
	}

	createRoaringResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "CREATERB",
		Key:     "ids",
	})
	if err != nil {
		t.Fatalf("Command(CREATERB) error = %v", err)
	}
	if !createRoaringResp.GetOk() {
		t.Fatalf("CREATERB response = %#v, want ok", createRoaringResp)
	}
	addRoaringResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "ADDRB",
		Key:     "ids",
		Values:  []string{"1", "65543"},
	})
	if err != nil {
		t.Fatalf("Command(ADDRB) error = %v", err)
	}
	if !addRoaringResp.GetOk() || addRoaringResp.GetValue() != "2" {
		t.Fatalf("ADDRB response = %#v, want added 2", addRoaringResp)
	}
	hasRoaringResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "HASRB",
		Key:     "ids",
		Value:   "65543",
	})
	if err != nil {
		t.Fatalf("Command(HASRB) error = %v", err)
	}
	if !hasRoaringResp.GetOk() || hasRoaringResp.GetValue() != "1" {
		t.Fatalf("HASRB response = %#v, want hit", hasRoaringResp)
	}
	removeRoaringResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "REMRB",
		Key:     "ids",
		Value:   "1",
	})
	if err != nil {
		t.Fatalf("Command(REMRB) error = %v", err)
	}
	if !removeRoaringResp.GetOk() || removeRoaringResp.GetValue() != "1" {
		t.Fatalf("REMRB response = %#v, want removed 1", removeRoaringResp)
	}
	countRoaringResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "COUNTRB",
		Key:     "ids",
	})
	if err != nil {
		t.Fatalf("Command(COUNTRB) error = %v", err)
	}
	if !countRoaringResp.GetOk() || countRoaringResp.GetValue() != "1" {
		t.Fatalf("COUNTRB response = %#v, want count 1", countRoaringResp)
	}
	getRoaringResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "GETRB",
		Key:     "ids",
	})
	if err != nil {
		t.Fatalf("Command(GETRB) error = %v", err)
	}
	if !getRoaringResp.GetOk() || getRoaringResp.GetValue() != "[65543]" {
		t.Fatalf("GETRB response = %#v, want remaining sorted integer", getRoaringResp)
	}
	infoRoaringResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "INFORB",
		Key:     "ids",
	})
	if err != nil {
		t.Fatalf("Command(INFORB) error = %v", err)
	}
	if !infoRoaringResp.GetOk() || infoRoaringResp.GetValue() == "" {
		t.Fatalf("INFORB response = %#v, want JSON info", infoRoaringResp)
	}
	var roaringInfo RoaringBitmapInfo
	if err := json.Unmarshal([]byte(infoRoaringResp.GetValue()), &roaringInfo); err != nil {
		t.Fatalf("INFORB JSON error = %v", err)
	}
	if roaringInfo.Cardinality != 1 || roaringInfo.Containers != 1 || roaringInfo.EncodedBytes != 2 {
		t.Fatalf("INFORB = %#v, want one compact bitmap value", roaringInfo)
	}

	createSparseResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "CREATESB",
		Key:     "ids64",
	})
	if err != nil {
		t.Fatalf("Command(CREATESB) error = %v", err)
	}
	if !createSparseResp.GetOk() {
		t.Fatalf("CREATESB response = %#v, want ok", createSparseResp)
	}
	addSparseResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "ADDSB",
		Key:     "ids64",
		Values:  []string{"1", "4294967303", "18446744073709551615"},
	})
	if err != nil {
		t.Fatalf("Command(ADDSB) error = %v", err)
	}
	if !addSparseResp.GetOk() || addSparseResp.GetValue() != "3" {
		t.Fatalf("ADDSB response = %#v, want added 3", addSparseResp)
	}
	hasSparseResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "HASSB",
		Key:     "ids64",
		Value:   "18446744073709551615",
	})
	if err != nil {
		t.Fatalf("Command(HASSB) error = %v", err)
	}
	if !hasSparseResp.GetOk() || hasSparseResp.GetValue() != "1" {
		t.Fatalf("HASSB response = %#v, want hit", hasSparseResp)
	}
	countSparseResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "COUNTSB",
		Key:     "ids64",
	})
	if err != nil {
		t.Fatalf("Command(COUNTSB) error = %v", err)
	}
	if !countSparseResp.GetOk() || countSparseResp.GetValue() != "3" {
		t.Fatalf("COUNTSB response = %#v, want count 3", countSparseResp)
	}
	infoSparseResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "INFOSB",
		Key:     "ids64",
	})
	if err != nil {
		t.Fatalf("Command(INFOSB) error = %v", err)
	}
	if !infoSparseResp.GetOk() || infoSparseResp.GetValue() == "" {
		t.Fatalf("INFOSB response = %#v, want JSON info", infoSparseResp)
	}
	var sparseInfo SparseBitsetInfo
	if err := json.Unmarshal([]byte(infoSparseResp.GetValue()), &sparseInfo); err != nil {
		t.Fatalf("INFOSB JSON error = %v", err)
	}
	if sparseInfo.Cardinality != 3 || sparseInfo.Containers != 3 || sparseInfo.EncodedBytes != 6 {
		t.Fatalf("INFOSB = %#v, want compact sparse bitset value", sparseInfo)
	}

	createRadixResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "CREATERT",
		Key:     "index",
	})
	if err != nil {
		t.Fatalf("Command(CREATERT) error = %v", err)
	}
	if !createRadixResp.GetOk() {
		t.Fatalf("CREATERT response = %#v, want ok", createRadixResp)
	}
	putRadixResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "PUTRT",
		Key:     "index",
		Subkey:  "user:100/profile",
		Value:   "active",
	})
	if err != nil {
		t.Fatalf("Command(PUTRT) error = %v", err)
	}
	if !putRadixResp.GetOk() || putRadixResp.GetValue() != "1" {
		t.Fatalf("PUTRT response = %#v, want added 1", putRadixResp)
	}
	getRadixResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "GETRT",
		Key:     "index",
		Subkey:  "user:100/profile",
	})
	if err != nil {
		t.Fatalf("Command(GETRT) error = %v", err)
	}
	if !getRadixResp.GetOk() || getRadixResp.GetValue() != "active" {
		t.Fatalf("GETRT response = %#v, want active", getRadixResp)
	}
	prefixRadixResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "PREFIXRT",
		Key:     "index",
		Subkey:  "user:",
	})
	if err != nil {
		t.Fatalf("Command(PREFIXRT) error = %v", err)
	}
	if !prefixRadixResp.GetOk() || prefixRadixResp.GetValue() == "" {
		t.Fatalf("PREFIXRT response = %#v, want JSON items", prefixRadixResp)
	}
	var radixItems []RadixTreeItem
	if err := json.Unmarshal([]byte(prefixRadixResp.GetValue()), &radixItems); err != nil {
		t.Fatalf("PREFIXRT JSON error = %v", err)
	}
	if got, want := radixTreeItemKeys(radixItems), []string{"user:100/profile"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("PREFIXRT keys = %#v, want %#v", got, want)
	}

	createSketchResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "CREATECMS",
		Key:     "freq",
		Value:   "128",
		Subkey:  "4",
	})
	if err != nil {
		t.Fatalf("Command(CREATECMS) error = %v", err)
	}
	if !createSketchResp.GetOk() {
		t.Fatalf("CREATECMS response = %#v, want ok", createSketchResp)
	}
	incrSketchResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "INCRCMS",
		Key:     "freq",
		Value:   "alpha",
		Subkey:  "3",
	})
	if err != nil {
		t.Fatalf("Command(INCRCMS) error = %v", err)
	}
	if !incrSketchResp.GetOk() || incrSketchResp.GetValue() != "3" {
		t.Fatalf("INCRCMS response = %#v, want estimate 3", incrSketchResp)
	}
	estimateSketchResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "ESTCMS",
		Key:     "freq",
		Value:   "alpha",
	})
	if err != nil {
		t.Fatalf("Command(ESTCMS) error = %v", err)
	}
	if !estimateSketchResp.GetOk() || estimateSketchResp.GetValue() != "3" {
		t.Fatalf("ESTCMS response = %#v, want estimate 3", estimateSketchResp)
	}
	infoSketchResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "INFOCMS",
		Key:     "freq",
	})
	if err != nil {
		t.Fatalf("Command(INFOCMS) error = %v", err)
	}
	if !infoSketchResp.GetOk() || infoSketchResp.GetValue() == "" {
		t.Fatalf("INFOCMS response = %#v, want JSON info", infoSketchResp)
	}

	createHLLResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "CREATEHLL",
		Key:     "card",
		Value:   "10",
	})
	if err != nil {
		t.Fatalf("Command(CREATEHLL) error = %v", err)
	}
	if !createHLLResp.GetOk() {
		t.Fatalf("CREATEHLL response = %#v, want ok", createHLLResp)
	}
	addHLLResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "ADDHLL",
		Key:     "card",
		Values:  []string{"alpha", "beta"},
	})
	if err != nil {
		t.Fatalf("Command(ADDHLL) error = %v", err)
	}
	if !addHLLResp.GetOk() || addHLLResp.GetValue() == "0" {
		t.Fatalf("ADDHLL response = %#v, want non-zero estimate", addHLLResp)
	}
	countHLLResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "COUNTHLL",
		Key:     "card",
	})
	if err != nil {
		t.Fatalf("Command(COUNTHLL) error = %v", err)
	}
	if !countHLLResp.GetOk() || countHLLResp.GetValue() != addHLLResp.GetValue() {
		t.Fatalf("COUNTHLL response = %#v, want estimate %q", countHLLResp, addHLLResp.GetValue())
	}
	infoHLLResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "INFOHLL",
		Key:     "card",
	})
	if err != nil {
		t.Fatalf("Command(INFOHLL) error = %v", err)
	}
	if !infoHLLResp.GetOk() || infoHLLResp.GetValue() == "" {
		t.Fatalf("INFOHLL response = %#v, want JSON info", infoHLLResp)
	}

	createTopKResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "CREATETOPK",
		Key:     "top",
		Value:   "3",
	})
	if err != nil {
		t.Fatalf("Command(CREATETOPK) error = %v", err)
	}
	if !createTopKResp.GetOk() {
		t.Fatalf("CREATETOPK response = %#v, want ok", createTopKResp)
	}
	addTopKResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "ADDTOPK",
		Key:     "top",
		Value:   "alpha",
		Subkey:  "5",
	})
	if err != nil {
		t.Fatalf("Command(ADDTOPK) error = %v", err)
	}
	if !addTopKResp.GetOk() || addTopKResp.GetValue() == "" {
		t.Fatalf("ADDTOPK response = %#v, want JSON estimate", addTopKResp)
	}
	getTopKResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "GETTOPK",
		Key:     "top",
	})
	if err != nil {
		t.Fatalf("Command(GETTOPK) error = %v", err)
	}
	if !getTopKResp.GetOk() || getTopKResp.GetValue() == "" {
		t.Fatalf("GETTOPK response = %#v, want JSON items", getTopKResp)
	}
	infoTopKResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "INFOTOPK",
		Key:     "top",
	})
	if err != nil {
		t.Fatalf("Command(INFOTOPK) error = %v", err)
	}
	if !infoTopKResp.GetOk() || infoTopKResp.GetValue() == "" {
		t.Fatalf("INFOTOPK response = %#v, want JSON info", infoTopKResp)
	}

	createReservoirResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "CREATERS",
		Key:     "sample",
		Value:   "3",
	})
	if err != nil {
		t.Fatalf("Command(CREATERS) error = %v", err)
	}
	if !createReservoirResp.GetOk() {
		t.Fatalf("CREATERS response = %#v, want ok", createReservoirResp)
	}
	addReservoirResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "ADDRS",
		Key:     "sample",
		Values:  []string{"alpha", "beta", "gamma", "delta"},
	})
	if err != nil {
		t.Fatalf("Command(ADDRS) error = %v", err)
	}
	if !addReservoirResp.GetOk() || addReservoirResp.GetValue() == "" {
		t.Fatalf("ADDRS response = %#v, want JSON update", addReservoirResp)
	}
	getReservoirResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "GETRS",
		Key:     "sample",
	})
	if err != nil {
		t.Fatalf("Command(GETRS) error = %v", err)
	}
	if !getReservoirResp.GetOk() || getReservoirResp.GetValue() == "" {
		t.Fatalf("GETRS response = %#v, want JSON items", getReservoirResp)
	}
	infoReservoirResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "INFORS",
		Key:     "sample",
	})
	if err != nil {
		t.Fatalf("Command(INFORS) error = %v", err)
	}
	if !infoReservoirResp.GetOk() || infoReservoirResp.GetValue() == "" {
		t.Fatalf("INFORS response = %#v, want JSON info", infoReservoirResp)
	}

	createQuantileResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "CREATEQ",
		Key:     "latency",
		Value:   "0.01",
	})
	if err != nil {
		t.Fatalf("Command(CREATEQ) error = %v", err)
	}
	if !createQuantileResp.GetOk() {
		t.Fatalf("CREATEQ response = %#v, want ok", createQuantileResp)
	}
	addQuantileResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "ADDQ",
		Key:     "latency",
		Values:  []string{"10", "20", "30"},
	})
	if err != nil {
		t.Fatalf("Command(ADDQ) error = %v", err)
	}
	if !addQuantileResp.GetOk() || addQuantileResp.GetValue() == "" {
		t.Fatalf("ADDQ response = %#v, want JSON estimate", addQuantileResp)
	}
	estimateQuantileResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "ESTQ",
		Key:     "latency",
		Value:   "0.5",
	})
	if err != nil {
		t.Fatalf("Command(ESTQ) error = %v", err)
	}
	if !estimateQuantileResp.GetOk() || estimateQuantileResp.GetValue() == "" {
		t.Fatalf("ESTQ response = %#v, want JSON estimate", estimateQuantileResp)
	}
	infoQuantileResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "INFOQ",
		Key:     "latency",
	})
	if err != nil {
		t.Fatalf("Command(INFOQ) error = %v", err)
	}
	if !infoQuantileResp.GetOk() || infoQuantileResp.GetValue() == "" {
		t.Fatalf("INFOQ response = %#v, want JSON info", infoQuantileResp)
	}

	createFenwickResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "CREATEFW",
		Key:     "scores",
		Value:   "8",
	})
	if err != nil {
		t.Fatalf("Command(CREATEFW) error = %v", err)
	}
	if !createFenwickResp.GetOk() {
		t.Fatalf("CREATEFW response = %#v, want ok", createFenwickResp)
	}
	addFenwickResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "ADDFW",
		Key:     "scores",
		Values:  []string{"2", "5"},
	})
	if err != nil {
		t.Fatalf("Command(ADDFW) error = %v", err)
	}
	if !addFenwickResp.GetOk() || addFenwickResp.GetValue() == "" {
		t.Fatalf("ADDFW response = %#v, want JSON update", addFenwickResp)
	}
	sumFenwickResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "SUMFW",
		Key:     "scores",
		Value:   "2",
	})
	if err != nil {
		t.Fatalf("Command(SUMFW) error = %v", err)
	}
	if !sumFenwickResp.GetOk() || sumFenwickResp.GetValue() != "5" {
		t.Fatalf("SUMFW response = %#v, want 5", sumFenwickResp)
	}
	rangeFenwickResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "RANGEFW",
		Key:     "scores",
		Values:  []string{"2", "4"},
	})
	if err != nil {
		t.Fatalf("Command(RANGEFW) error = %v", err)
	}
	if !rangeFenwickResp.GetOk() || rangeFenwickResp.GetValue() != "5" {
		t.Fatalf("RANGEFW response = %#v, want 5", rangeFenwickResp)
	}
	infoFenwickResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "INFOFW",
		Key:     "scores",
	})
	if err != nil {
		t.Fatalf("Command(INFOFW) error = %v", err)
	}
	if !infoFenwickResp.GetOk() || infoFenwickResp.GetValue() == "" {
		t.Fatalf("INFOFW response = %#v, want JSON info", infoFenwickResp)
	}

	stats, err := client.Stats(context.Background(), &hatriecachev1.StatsRequest{})
	if err != nil {
		t.Fatalf("Stats() error = %v", err)
	}
	if stats.GetWrites() == 0 || stats.GetReads() == 0 {
		t.Fatalf("Stats() = %#v, want reads and writes", stats)
	}
	if stats.GetLastWriteUnixNano() == 0 {
		t.Fatalf("LastWriteUnixNano = 0, want write timestamp")
	}

	entries, err := client.Entries(context.Background(), &hatriecachev1.EntriesRequest{Prefix: "session:"})
	if err != nil {
		t.Fatalf("Entries() error = %v", err)
	}
	if len(entries.GetEntries()) != 1 {
		t.Fatalf("entries len = %d, want 1: %#v", len(entries.GetEntries()), entries.GetEntries())
	}
	entry := entries.GetEntries()[0]
	if entry.GetKey() != "session:1" || entry.GetType() != "string" || entry.GetValuePreview() != "active user" {
		t.Fatalf("entry = %#v, want session string", entry)
	}
	if entry.TtlMillis == nil || entry.GetTtlMillis() != int64(time.Minute/time.Millisecond) {
		t.Fatalf("entry ttl = %v, want 60000", entry.TtlMillis)
	}
}

func TestCacheGRPCServerCommandStreamExecutesPipelinedCommandsInOrder(t *testing.T) {
	ht := newTestTrie(t)
	client, stop := newTestGRPCClient(t, ht, CacheGRPCOptions{})
	defer stop()
	stream, err := client.CommandStream(context.Background())
	if err != nil {
		t.Fatalf("CommandStream() error = %v", err)
	}
	requests := []*hatriecachev1.CommandRequest{
		{Command: "SETSTR", Key: "name", Value: "ivi"},
		{Command: "SETINT", Key: "count", Value: "40"},
		{Command: "INC", Key: "count", Value: "2"},
		{Command: "GETSTR", Key: "name"},
	}
	for _, request := range requests {
		if err := stream.Send(request); err != nil {
			t.Fatalf("CommandStream.Send(%s) error = %v", request.Command, err)
		}
	}
	for index := range requests {
		response, err := stream.Recv()
		if err != nil {
			t.Fatalf("CommandStream.Recv(%d) error = %v", index, err)
		}
		if !response.GetOk() {
			t.Fatalf("CommandStream.Recv(%d) = %#v, want ok", index, response)
		}
		switch index {
		case 2:
			if response.GetValue() != "42" {
				t.Fatalf("INC response value = %q, want 42", response.GetValue())
			}
		case 3:
			if response.GetValue() != "ivi" {
				t.Fatalf("GETSTR response value = %q, want ivi", response.GetValue())
			}
		}
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("CommandStream.CloseSend() error = %v", err)
	}
	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("CommandStream final Recv() error = %v, want EOF", err)
	}
}

func TestCacheGRPCServerCommandStreamRequiresAuthentication(t *testing.T) {
	ht := newTestTrie(t)
	client, stop := newTestGRPCClient(t, ht, CacheGRPCOptions{AuthToken: "secret"})
	defer stop()
	unauthorized, err := client.CommandStream(context.Background())
	if err != nil {
		t.Fatalf("CommandStream(unauthorized) open error = %v", err)
	}
	if err := unauthorized.Send(&hatriecachev1.CommandRequest{Command: "GET", Key: "name"}); err != nil {
		t.Fatalf("CommandStream(unauthorized) send error = %v", err)
	}
	if _, err := unauthorized.Recv(); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("CommandStream(unauthorized) recv error = %v, want Unauthenticated", err)
	}

	ctx := metadata.AppendToOutgoingContext(context.Background(), "x-hatrie-auth-token", "secret")
	authorized, err := client.CommandStream(ctx)
	if err != nil {
		t.Fatalf("CommandStream(authorized) open error = %v", err)
	}
	if err := authorized.Send(&hatriecachev1.CommandRequest{Command: "SETSTR", Key: "name", Value: "ivi"}); err != nil {
		t.Fatalf("CommandStream(authorized) send error = %v", err)
	}
	response, err := authorized.Recv()
	if err != nil || !response.GetOk() {
		t.Fatalf("CommandStream(authorized) response = %#v/%v, want ok", response, err)
	}
	if err := authorized.CloseSend(); err != nil {
		t.Fatalf("CommandStream(authorized) close error = %v", err)
	}
}

func TestCacheGRPCServerCommandStreamHonorsWriteProtection(t *testing.T) {
	ht := newTestTrie(t)
	ht.UpsertString("name", "original")
	client, stop := newTestGRPCClient(t, ht, CacheGRPCOptions{WriteProtected: true})
	defer stop()
	stream, err := client.CommandStream(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&hatriecachev1.CommandRequest{Command: "GETSTR", Key: "name"}); err != nil {
		t.Fatal(err)
	}
	if response, err := stream.Recv(); err != nil || !response.GetOk() || response.GetValue() != "original" {
		t.Fatalf("streamed read = %#v/%v, want original", response, err)
	}
	if err := stream.Send(&hatriecachev1.CommandRequest{Command: "SETSTR", Key: "name", Value: "changed"}); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("streamed write error = %v, want PermissionDenied", err)
	}
	if got := ht.GetString("name"); got != "original" {
		t.Fatalf("write-protected value = %q, want original", got)
	}
}

func TestCacheGRPCServerCommandStreamContinuesAfterCommandError(t *testing.T) {
	ht := newTestTrie(t)
	ht.UpsertString("name", "ivi")
	client, stop := newTestGRPCClient(t, ht, CacheGRPCOptions{})
	defer stop()
	stream, err := client.CommandStream(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&hatriecachev1.CommandRequest{Command: "NOTACOMMAND", Key: "invalid"}); err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&hatriecachev1.CommandRequest{Command: "GETSTR", Key: "name"}); err != nil {
		t.Fatal(err)
	}
	failed, err := stream.Recv()
	if err != nil || failed.GetOk() || !strings.Contains(failed.GetMessage(), "unsupported command") {
		t.Fatalf("invalid streamed command = %#v/%v, want application error response", failed, err)
	}
	succeeded, err := stream.Recv()
	if err != nil || !succeeded.GetOk() || succeeded.GetValue() != "ivi" {
		t.Fatalf("stream command after error = %#v/%v, want ivi", succeeded, err)
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatal(err)
	}
}

func TestCacheGRPCServerCommandStreamAuditsStreamMethod(t *testing.T) {
	ht := newTestTrie(t)
	var audit bytes.Buffer
	client, stop := newTestGRPCClient(t, ht, CacheGRPCOptions{AuditLog: NewAuditLogger(&audit)})
	defer stop()
	stream, err := client.CommandStream(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&hatriecachev1.CommandRequest{Command: "SETSTR", Key: "name", Value: "secret-value"}); err != nil {
		t.Fatal(err)
	}
	if response, err := stream.Recv(); err != nil || !response.GetOk() {
		t.Fatalf("streamed write = %#v/%v, want ok", response, err)
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatal(err)
	}
	events := auditEventsFromJSONL(t, audit.String())
	if len(events) != 1 || events[0].Method != "/hatriecache.v1.CacheService/CommandStream" || events[0].Command != "SETSTR" || !events[0].OK {
		t.Fatalf("stream audit events = %#v, want one successful CommandStream event", events)
	}
	if strings.Contains(audit.String(), "secret-value") {
		t.Fatalf("stream audit leaked command value: %s", audit.String())
	}
}

func TestCacheGRPCServerLimitsEntries(t *testing.T) {
	ht := newTestTrie(t)
	ht.UpsertString("session:1", "one")
	ht.UpsertString("session:2", "two")
	ht.UpsertString("session:3", "three")
	ht.UpsertString("other:1", "ignored")
	client, stop := newTestGRPCClient(t, ht, CacheGRPCOptions{})
	defer stop()

	limited, err := client.Entries(context.Background(), &hatriecachev1.EntriesRequest{
		Prefix: "session:",
		Limit:  2,
	})
	if err != nil {
		t.Fatalf("Entries(limit=2) error = %v", err)
	}
	if limited.GetLimit() != 2 || !limited.GetHasMore() {
		t.Fatalf("limited entries metadata = limit %d has_more %v, want 2/true", limited.GetLimit(), limited.GetHasMore())
	}
	if limited.GetNextAfterKey() != "session:2" {
		t.Fatalf("limited entries next_after_key = %q, want session:2", limited.GetNextAfterKey())
	}
	if got := grpcEntryKeys(limited.GetEntries()); !reflect.DeepEqual(got, []string{"session:1", "session:2"}) {
		t.Fatalf("limited entries keys = %#v, want first two sorted session keys", got)
	}

	afterKey := limited.GetNextAfterKey()
	next, err := client.Entries(context.Background(), &hatriecachev1.EntriesRequest{
		Prefix:   "session:",
		Limit:    2,
		AfterKey: &afterKey,
	})
	if err != nil {
		t.Fatalf("Entries(next page) error = %v", err)
	}
	if next.GetAfterKey() != "session:2" || next.GetHasMore() || next.GetNextAfterKey() != "" {
		t.Fatalf("next entries metadata = after %q has_more %v next %q, want session:2/false/empty", next.GetAfterKey(), next.GetHasMore(), next.GetNextAfterKey())
	}
	if got := grpcEntryKeys(next.GetEntries()); !reflect.DeepEqual(got, []string{"session:3"}) {
		t.Fatalf("next entries keys = %#v, want remaining session key", got)
	}

	exact, err := client.Entries(context.Background(), &hatriecachev1.EntriesRequest{
		Prefix: "session:",
		Limit:  3,
	})
	if err != nil {
		t.Fatalf("Entries(limit=3) error = %v", err)
	}
	if exact.GetLimit() != 3 || exact.GetHasMore() {
		t.Fatalf("exact entries metadata = limit %d has_more %v, want 3/false", exact.GetLimit(), exact.GetHasMore())
	}
	if got := grpcEntryKeys(exact.GetEntries()); !reflect.DeepEqual(got, []string{"session:1", "session:2", "session:3"}) {
		t.Fatalf("exact entries keys = %#v, want all sorted session keys", got)
	}
}

func TestCacheGRPCServerPagesAfterEmptyKey(t *testing.T) {
	ht := newTestTrie(t)
	ht.UpsertString("", "empty")
	ht.UpsertString("alpha", "one")
	client, stop := newTestGRPCClient(t, ht, CacheGRPCOptions{})
	defer stop()

	limited, err := client.Entries(context.Background(), &hatriecachev1.EntriesRequest{Limit: 1})
	if err != nil {
		t.Fatalf("Entries(limit=1) error = %v", err)
	}
	if !limited.GetHasMore() || limited.GetNextAfterKey() != "" {
		t.Fatalf("limited entries metadata = has_more %v next %q, want true/empty", limited.GetHasMore(), limited.GetNextAfterKey())
	}
	if got := grpcEntryKeys(limited.GetEntries()); !reflect.DeepEqual(got, []string{""}) {
		t.Fatalf("limited entries keys = %#v, want empty key", got)
	}

	emptyCursor := limited.GetNextAfterKey()
	next, err := client.Entries(context.Background(), &hatriecachev1.EntriesRequest{
		Limit:    1,
		AfterKey: &emptyCursor,
	})
	if err != nil {
		t.Fatalf("Entries(after empty key) error = %v", err)
	}
	if next.GetAfterKey() != "" || next.GetHasMore() || next.GetNextAfterKey() != "" {
		t.Fatalf("next entries metadata = after %q has_more %v next %q, want empty/false/empty", next.GetAfterKey(), next.GetHasMore(), next.GetNextAfterKey())
	}
	if got := grpcEntryKeys(next.GetEntries()); !reflect.DeepEqual(got, []string{"alpha"}) {
		t.Fatalf("next entries keys = %#v, want key after empty cursor", got)
	}
}

func TestCacheGRPCServerRejectsOversizedEntriesLimit(t *testing.T) {
	ht := newTestTrie(t)
	client, stop := newTestGRPCClient(t, ht, CacheGRPCOptions{})
	defer stop()

	_, err := client.Entries(context.Background(), &hatriecachev1.EntriesRequest{
		Limit: maxMonitoringEntriesLimit + 1,
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Entries(oversized limit) error = %v, want InvalidArgument", err)
	}
	afterKey := "other:1"
	_, err = client.Entries(context.Background(), &hatriecachev1.EntriesRequest{
		Prefix:   "session:",
		AfterKey: &afterKey,
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Entries(cursor prefix mismatch) error = %v, want InvalidArgument", err)
	}
}

func TestCacheGRPCServerAcceptsGzipCompressedCalls(t *testing.T) {
	ht := newTestTrie(t)
	client, stop := newTestGRPCClient(t, ht, CacheGRPCOptions{
		NodeName: "test-node",
	})
	defer stop()

	value := strings.Repeat("value", 64)
	resp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "SETSTR",
		Key:     "compressed",
		Value:   value,
	}, grpc.UseCompressor("gzip"))
	if err != nil {
		t.Fatalf("Command(gzip) error = %v", err)
	}
	if !resp.GetOk() {
		t.Fatalf("Command(gzip) response = %#v, want ok", resp)
	}
	if got := ht.GetString("compressed"); got != value {
		t.Fatalf("compressed value = %q, want %q", got, value)
	}

	health, err := client.Health(context.Background(), &hatriecachev1.HealthRequest{}, grpc.UseCompressor("gzip"))
	if err != nil {
		t.Fatalf("Health(gzip) error = %v", err)
	}
	if health.GetStatus() != "online" || health.GetNode() != "test-node" {
		t.Fatalf("Health(gzip) = %#v, want online test-node", health)
	}
}

func TestCacheGRPCServerAuthTokenProtectsRPCs(t *testing.T) {
	ht := newTestTrie(t)
	client, stop := newTestGRPCClient(t, ht, CacheGRPCOptions{AuthToken: "secret"})
	defer stop()

	_, err := client.Health(context.Background(), &hatriecachev1.HealthRequest{})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("Health(unauthenticated) error = %v, want Unauthenticated", err)
	}

	wrongToken := metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer wrong")
	_, err = client.Stats(wrongToken, &hatriecachev1.StatsRequest{})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("Stats(wrong token) error = %v, want Unauthenticated", err)
	}

	bearerToken := metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer secret")
	health, err := client.Health(bearerToken, &hatriecachev1.HealthRequest{})
	if err != nil {
		t.Fatalf("Health(bearer token) error = %v", err)
	}
	if health.GetStatus() != "online" {
		t.Fatalf("Health(bearer token) status = %q, want online", health.GetStatus())
	}

	headerToken := metadata.AppendToOutgoingContext(context.Background(), "x-hatrie-auth-token", "secret")
	stats, err := client.Stats(headerToken, &hatriecachev1.StatsRequest{})
	if err != nil {
		t.Fatalf("Stats(header token) error = %v", err)
	}
	if stats.GetReads() != 0 {
		t.Fatalf("Stats(header token) reads = %d, want 0", stats.GetReads())
	}
}

func TestCacheGRPCServerAuditLogRecordsDangerousRPCs(t *testing.T) {
	ht := newTestTrie(t)
	var audit bytes.Buffer
	metrics := NewAPIMetrics()
	client, stop := newTestGRPCClient(t, ht, CacheGRPCOptions{
		AuditLog: NewAuditLogger(&audit),
		Metrics:  metrics,
	})
	defer stop()

	resp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "SETSTR",
		Key:     "name",
		Value:   "ivi",
	})
	if err != nil {
		t.Fatalf("Command(SETSTR) error = %v", err)
	}
	if !resp.GetOk() {
		t.Fatalf("Command(SETSTR) response = %#v, want ok", resp)
	}

	events := auditEventsFromJSONL(t, audit.String())
	if len(events) != 1 {
		t.Fatalf("audit events = %#v, want one command event", events)
	}
	if events[0].Protocol != "grpc" || events[0].Action != "command" || events[0].Command != "SETSTR" || events[0].Key != "name" || !events[0].OK {
		t.Fatalf("gRPC audit event = %#v, want safe command event", events[0])
	}
	if strings.Contains(audit.String(), "ivi") {
		t.Fatalf("audit log leaked command value: %s", audit.String())
	}
	if got := metrics.Snapshot(); got.AuditEventsTotal != 1 || got.AuditErrorsTotal != 0 {
		t.Fatalf("gRPC audit metrics = %#v, want one audit event and no errors", got)
	}
}

func TestCacheGRPCServerWriteProtectionRejectsDangerousRPCs(t *testing.T) {
	ht := newTestTrie(t)
	metrics := NewAPIMetrics()
	client, stop := newTestGRPCClient(t, ht, CacheGRPCOptions{WriteProtected: true, Metrics: metrics})
	defer stop()

	_, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "SETSTR",
		Key:     "name",
		Value:   "ivi",
	})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("Command(write protected) error = %v, want PermissionDenied", err)
	}
	if got := ht.GetString("name"); got != "" {
		t.Fatalf("write-protected gRPC command wrote value %q, want empty", got)
	}
	if got := metrics.Snapshot(); got.WriteProtectionRejectionsTotal != 1 {
		t.Fatalf("gRPC write protection metrics = %#v, want one rejection", got)
	}
}

func TestCacheGRPCServerRateLimitRejectsDangerousRPCs(t *testing.T) {
	ht := newTestTrie(t)
	metrics := NewAPIMetrics()
	client, stop := newTestGRPCClient(t, ht, CacheGRPCOptions{RateLimiter: NewRateLimiter(1, time.Second), Metrics: metrics})
	defer stop()

	if _, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{Command: "SETSTR", Key: "one", Value: "1"}); err != nil {
		t.Fatalf("first Command() error = %v", err)
	}
	_, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{Command: "SETSTR", Key: "two", Value: "2"})
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("second Command() error = %v, want ResourceExhausted", err)
	}
	if got := metrics.Snapshot(); got.RateLimitRejectionsTotal != 1 {
		t.Fatalf("gRPC rate limit metrics = %#v, want one rejection", got)
	}
}

func TestCacheGRPCServerSnapshotAndJournal(t *testing.T) {
	ht := newTestTrie(t)
	journal, err := OpenCommandJournal(filepath.Join(t.TempDir(), "commands.journal"))
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}
	snapshotCalled := false
	client, stop := newTestGRPCClient(t, ht, CacheGRPCOptions{
		Journal: journal,
		Snapshot: func() error {
			snapshotCalled = true
			return nil
		},
	})
	defer stop()

	resp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "SETSTR",
		Key:     "name",
		Value:   "ivi",
	})
	if err != nil {
		t.Fatalf("Command(SETSTR) error = %v", err)
	}
	if !resp.GetOk() {
		t.Fatalf("SETSTR response = %#v, want ok", resp)
	}
	if journal.Sequence() != 1 {
		t.Fatalf("journal sequence = %d, want 1", journal.Sequence())
	}

	snapshotResp, err := client.Snapshot(context.Background(), &hatriecachev1.SnapshotRequest{})
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	if !snapshotResp.GetOk() || !snapshotCalled {
		t.Fatalf("Snapshot response/called = %#v/%v, want ok true", snapshotResp, snapshotCalled)
	}
}

func TestCacheGRPCServerEnforcesLeaderWrites(t *testing.T) {
	topology := replicationTestTopology(t, "127.0.0.1:1")
	election := NewElectionStore(topology, ElectionOptions{})
	if err := election.MarkOffline("node-a"); err != nil {
		t.Fatalf("MarkOffline(node-a) error = %v", err)
	}

	followerTrie := newTestTrie(t)
	followerClient, followerStop := newTestGRPCClient(t, followerTrie, CacheGRPCOptions{
		NodeName:            "node-a",
		Election:            election,
		EnforceLeaderWrites: true,
	})
	defer followerStop()

	rejected, err := followerClient.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "SETSTR",
		Key:     "session:1",
		Value:   "value",
	})
	if err != nil {
		t.Fatalf("follower Command(SETSTR) error = %v", err)
	}
	if rejected.GetOk() || !strings.Contains(rejected.GetMessage(), "leader is node-b") {
		t.Fatalf("follower SETSTR response = %#v, want leader rejection", rejected)
	}
	if got := followerTrie.GetString("session:1"); got != "" {
		t.Fatalf("follower wrote value %q, want no local write", got)
	}

	internal, err := followerClient.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "INTERNALSET",
		Key:     "session:1",
		Value:   `{"type":"string","string":"replicated"}`,
	})
	if err != nil {
		t.Fatalf("follower Command(INTERNALSET) error = %v", err)
	}
	if !internal.GetOk() {
		t.Fatalf("follower INTERNALSET response = %#v, want ok", internal)
	}
	if got := followerTrie.GetString("session:1"); got != "replicated" {
		t.Fatalf("internal replicated value = %q, want replicated", got)
	}

	leaderTrie := newTestTrie(t)
	leaderClient, leaderStop := newTestGRPCClient(t, leaderTrie, CacheGRPCOptions{
		NodeName:            "node-b",
		Election:            election,
		EnforceLeaderWrites: true,
	})
	defer leaderStop()
	stored, err := leaderClient.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "SETSTR",
		Key:     "session:1",
		Value:   "leader-value",
	})
	if err != nil {
		t.Fatalf("leader Command(SETSTR) error = %v", err)
	}
	if !stored.GetOk() {
		t.Fatalf("leader SETSTR response = %#v, want ok", stored)
	}
	if got := leaderTrie.GetString("session:1"); got != "leader-value" {
		t.Fatalf("leader wrote value %q, want leader-value", got)
	}
}

func TestCacheGRPCServerReplicatesCommands(t *testing.T) {
	requests := make(chan CacheCommandRequest, 1)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/commands" {
			t.Fatalf("path = %s, want /api/commands", r.URL.Path)
		}
		request := mustDecodeReplicationTestCommand(t, w, r)
		requests <- request
		writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	defer target.Close()

	ht := newTestTrie(t)
	topology := replicationTestTopology(t, target.URL)
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:     "node-a",
		Topology: topology,
		Election: election,
		Client:   target.Client(),
	})
	client, stop := newTestGRPCClient(t, ht, CacheGRPCOptions{
		NodeName:   "node-a",
		Election:   election,
		Replicator: replicator,
	})
	defer stop()

	response, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "SETSTR",
		Key:     "session:1",
		Value:   "value",
	})
	if err != nil {
		t.Fatalf("Command(SETSTR) error = %v", err)
	}
	if !response.GetOk() {
		t.Fatalf("SETSTR response = %#v, want ok", response)
	}
	select {
	case request := <-requests:
		if !isTypedReplicationSetPayload(request, "session:1") {
			t.Fatalf("replicated request = %#v, want typed binary snapshot", request)
		}
	default:
		t.Fatal("gRPC write did not reach replication target")
	}

	replication, err := client.Replication(context.Background(), &hatriecachev1.ReplicationRequest{})
	if err != nil {
		t.Fatalf("Replication() error = %v", err)
	}
	if replication.GetSkipped() || replication.GetCommand() != "SETSTR" || replication.GetKey() != "session:1" {
		t.Fatalf("replication status = %#v, want SETSTR session:1", replication)
	}
	if len(replication.GetTargets()) != 1 || !replication.GetTargets()[0].GetOk() {
		t.Fatalf("replication targets = %#v, want one ok target", replication.GetTargets())
	}
	if replication.GetHealth() != "ok" || replication.GetHealthScore() != 100 {
		t.Fatalf("replication health = %s/%d, want ok 100", replication.GetHealth(), replication.GetHealthScore())
	}
	if replication.GetStartedAtUnixNano() <= 0 || replication.GetFinishedAtUnixNano() < replication.GetStartedAtUnixNano() || replication.GetDurationMillis() < 0 {
		t.Fatalf("replication timing = started %d finished %d duration %d, want ordered timestamps", replication.GetStartedAtUnixNano(), replication.GetFinishedAtUnixNano(), replication.GetDurationMillis())
	}
}

func TestReplicationGRPCStreamSyncPreservesPagedOrder(t *testing.T) {
	targetTrie := newTestTrie(t)
	listener := bufconn.Listen(testGRPCBufferSize)
	server := grpc.NewServer()
	digestServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request := mustDecodeReplicationTestCommand(t, w, r)
		if !rejectReplicationDigestTestCommand(t, w, r, request) {
			t.Fatalf("HTTP replication request = %#v, want digest capability check", request)
		}
	}))
	defer digestServer.Close()

	topology, err := NewTopologyStore(ClusterTopology{
		Version: 1,
		Self:    "node-a",
		Nodes: []TopologyNode{
			{ID: "node-a", Address: "http://node-a"},
			{ID: "node-b", Address: digestServer.URL, GRPCAddress: "bufnet"},
		},
		Shards: []TopologyShard{{ID: 0, Primary: "node-a", Replicas: []string{"node-b"}}},
	})
	if err != nil {
		t.Fatalf("NewTopologyStore() error = %v", err)
	}
	RegisterCacheGRPCServer(server, NewCacheGRPCServer(targetTrie, CacheGRPCOptions{
		NodeName:             "node-b",
		ReplicationAuthToken: "replica-secret",
		Topology:             topology,
		ReplicationSafety:    NewReplicationSafetyStore(),
	}))
	go func() {
		if err := server.Serve(listener); err != nil && err != grpc.ErrServerStopped {
			t.Errorf("grpc Serve() error = %v", err)
		}
	}()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})

	sourceTrie := newTestTrie(t)
	for idx := 0; idx < 5; idx++ {
		sourceTrie.UpsertString(fmt.Sprintf("session:%d", idx), fmt.Sprintf("value-%d", idx))
	}
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:                "node-a",
		Topology:            topology,
		Election:            NewElectionStore(topology, ElectionOptions{}),
		Transport:           ReplicationTransportGRPCStream,
		DisableHTTPFallback: true,
		AuthToken:           "replica-secret",
		GRPCDialOptions: []grpc.DialOption{
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	})
	t.Cleanup(replicator.Close)

	result := replicator.syncAllPaged(context.Background(), sourceTrie, "session:", 2)
	if result.Skipped || result.Entries != 5 || len(result.Targets) == 0 {
		t.Fatalf("syncAllPaged() = %#v, want five streamed entries", result)
	}
	for _, target := range result.Targets {
		if !target.OK {
			t.Fatalf("syncAllPaged() target = %#v, want successful gRPC stream", target)
		}
	}
	for idx := 0; idx < 5; idx++ {
		key := fmt.Sprintf("session:%d", idx)
		if got, want := targetTrie.GetString(key), fmt.Sprintf("value-%d", idx); got != want {
			t.Fatalf("target %s = %q, want %q", key, got, want)
		}
	}
	if got := replicator.grpcStreamBatches.Load(); got != 1 {
		t.Fatalf("gRPC stream batches = %d, want one bounded ordered batch", got)
	}

	unauthorized := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:                "node-a",
		Topology:            topology,
		Election:            NewElectionStore(topology, ElectionOptions{}),
		Transport:           ReplicationTransportGRPCStream,
		DisableHTTPFallback: true,
		GRPCDialOptions: []grpc.DialOption{
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	})
	t.Cleanup(unauthorized.Close)
	unauthorizedResult := unauthorized.syncAllPaged(context.Background(), sourceTrie, "session:", 2)
	if len(unauthorizedResult.Targets) == 0 || !strings.Contains(strings.ToLower(unauthorizedResult.Targets[0].Error), "unauthenticated") {
		t.Fatalf("unauthorized stream targets = %#v, want authentication failure", unauthorizedResult.Targets)
	}
}

func TestReplicationGRPCStreamLiveSetDeleteReusesConnection(t *testing.T) {
	targetTrie := newTestTrie(t)
	listener := bufconn.Listen(testGRPCBufferSize)
	server := grpc.NewServer()
	httpRequests := atomic.Int32{}
	httpFallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		httpRequests.Add(1)
		http.Error(w, "unexpected HTTP fallback", http.StatusServiceUnavailable)
	}))
	defer httpFallback.Close()
	topology, err := NewTopologyStore(ClusterTopology{
		Version: 1,
		Self:    "node-a",
		Nodes: []TopologyNode{
			{ID: "node-a", Address: "http://node-a"},
			{ID: "node-b", Address: httpFallback.URL, GRPCAddress: "bufnet"},
		},
		Shards: []TopologyShard{{ID: 0, Primary: "node-a", Replicas: []string{"node-b"}}},
	})
	if err != nil {
		t.Fatalf("NewTopologyStore() error = %v", err)
	}
	RegisterCacheGRPCServer(server, NewCacheGRPCServer(targetTrie, CacheGRPCOptions{
		NodeName:             "node-b",
		ReplicationAuthToken: "replica-secret",
		Topology:             topology,
		ReplicationSafety:    NewReplicationSafetyStore(),
	}))
	go func() {
		if err := server.Serve(listener); err != nil && err != grpc.ErrServerStopped {
			t.Errorf("grpc Serve() error = %v", err)
		}
	}()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})

	dials := atomic.Int32{}
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:                "node-a",
		Topology:            topology,
		Election:            NewElectionStore(topology, ElectionOptions{}),
		Transport:           ReplicationTransportGRPCStream,
		GRPCStreamWindow:    4,
		DisableHTTPFallback: true,
		AuthToken:           "replica-secret",
		GRPCDialOptions: []grpc.DialOption{
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				dials.Add(1)
				return listener.Dial()
			}),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	})
	t.Cleanup(replicator.Close)

	sourceTrie := newTestTrie(t)
	sourceTrie.UpsertString("live:key", "value")
	setResult := replicator.ReplicateCommand(context.Background(), sourceTrie,
		CacheCommandRequest{Command: "SETSTR", Key: "live:key", Value: "value"},
		CacheCommandResponse{OK: true})
	if len(setResult.Targets) != 1 || !setResult.Targets[0].OK {
		t.Fatalf("live SET replication = %#v, want one successful gRPC target", setResult)
	}
	if got := targetTrie.GetString("live:key"); got != "value" {
		t.Fatalf("target live:key = %q, want value", got)
	}

	sourceTrie.Delete("live:key")
	deleteResult := replicator.ReplicateCommand(context.Background(), sourceTrie,
		CacheCommandRequest{Command: "DEL", Key: "live:key"}, CacheCommandResponse{OK: true})
	if len(deleteResult.Targets) != 1 || !deleteResult.Targets[0].OK {
		t.Fatalf("live DEL replication = %#v, want one successful gRPC target", deleteResult)
	}
	if targetTrie.Exists("live:key") {
		t.Fatal("target live:key still exists after streamed delete")
	}
	if got := dials.Load(); got != 1 {
		t.Fatalf("gRPC dials = %d, want one persistent live connection", got)
	}
	if got := httpRequests.Load(); got != 0 {
		t.Fatalf("HTTP fallback requests = %d, want none", got)
	}
	if got := replicator.grpcStreamBatches.Load(); got != 2 {
		t.Fatalf("gRPC stream batches = %d, want SET and DEL batches", got)
	}
}

func TestReplicationGRPCStreamLiveMicroBatchesConcurrentCommands(t *testing.T) {
	for _, test := range []struct {
		name        string
		maxCommands int
		wantBatches uint64
	}{
		{name: "groups four commands", maxCommands: 4, wantBatches: 3},
		{name: "one disables grouping", maxCommands: 1, wantBatches: 12},
	} {
		t.Run(test.name, func(t *testing.T) {
			targetTrie := newTestTrie(t)
			listener := bufconn.Listen(testGRPCBufferSize)
			server := grpc.NewServer()
			topology, err := NewTopologyStore(ClusterTopology{
				Version: 1,
				Self:    "node-a",
				Nodes: []TopologyNode{
					{ID: "node-a", Address: "http://node-a"},
					{ID: "node-b", Address: "http://node-b", GRPCAddress: "bufnet"},
				},
				Shards: []TopologyShard{{ID: 0, Primary: "node-a", Replicas: []string{"node-b"}}},
			})
			if err != nil {
				t.Fatalf("NewTopologyStore() error = %v", err)
			}
			RegisterCacheGRPCServer(server, NewCacheGRPCServer(targetTrie, CacheGRPCOptions{
				NodeName:          "node-b",
				Topology:          topology,
				ReplicationSafety: NewReplicationSafetyStore(),
			}))
			go func() {
				if err := server.Serve(listener); err != nil && err != grpc.ErrServerStopped {
					t.Errorf("grpc Serve() error = %v", err)
				}
			}()
			t.Cleanup(func() {
				server.Stop()
				_ = listener.Close()
			})

			replicator := NewHTTPReplicator(HTTPReplicatorOptions{
				Self:                     "node-a",
				Topology:                 topology,
				Election:                 NewElectionStore(topology, ElectionOptions{}),
				Transport:                ReplicationTransportGRPCStream,
				GRPCStreamWindow:         8,
				GRPCLiveBatchMaxCommands: test.maxCommands,
				GRPCLiveBatchWindow:      50 * time.Millisecond,
				DisableHTTPFallback:      true,
				GRPCDialOptions: []grpc.DialOption{
					grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
					grpc.WithTransportCredentials(insecure.NewCredentials()),
				},
			})
			t.Cleanup(replicator.Close)
			sourceTrie := newTestTrie(t)
			sourceTrie.UpsertString("warmup", "value")
			warmup := replicator.ReplicateCommand(context.Background(), sourceTrie,
				CacheCommandRequest{Command: "SETSTR", Key: "warmup", Value: "value"}, CacheCommandResponse{OK: true})
			if len(warmup.Targets) != 1 || !warmup.Targets[0].OK {
				t.Fatalf("warmup replication = %#v, want success", warmup)
			}
			replicator.grpcStreamBatches.Store(0)

			const commands = 12
			for idx := 0; idx < commands; idx++ {
				sourceTrie.UpsertString(fmt.Sprintf("micro:%02d", idx), "value")
			}
			start := make(chan struct{})
			results := make(chan ReplicationResult, commands)
			var callers sync.WaitGroup
			callers.Add(commands)
			for idx := 0; idx < commands; idx++ {
				key := fmt.Sprintf("micro:%02d", idx)
				go func() {
					defer callers.Done()
					<-start
					results <- replicator.ReplicateCommand(context.Background(), sourceTrie,
						CacheCommandRequest{Command: "SETSTR", Key: key, Value: "value"}, CacheCommandResponse{OK: true})
				}()
			}
			close(start)
			callers.Wait()
			close(results)
			for result := range results {
				if len(result.Targets) != 1 || !result.Targets[0].OK {
					t.Fatalf("micro-batched replication result = %#v, want success", result)
				}
			}
			if got := replicator.grpcStreamBatches.Load(); got != test.wantBatches {
				t.Fatalf("gRPC stream batches = %d, want %d", got, test.wantBatches)
			}
			for idx := 0; idx < commands; idx++ {
				key := fmt.Sprintf("micro:%02d", idx)
				if got := targetTrie.GetString(key); got != "value" {
					t.Fatalf("target %s = %q, want value", key, got)
				}
			}
		})
	}
}

func TestReplicationGRPCStreamCollectFlightDefersIncompatibleOrOversizedJob(t *testing.T) {
	newJob := func(source string, key string, valueBytes int) *replicationGRPCStreamJob {
		return &replicationGRPCStreamJob{
			ctx: context.Background(),
			request: &hatriecachev1.ReplicationStreamBatch{
				Source: source, Keys: []string{key}, BinaryValues: [][]byte{make([]byte, valueBytes)},
			},
			result: make(chan replicationGRPCStreamJobResult, 1),
		}
	}
	for _, test := range []struct {
		name  string
		first *replicationGRPCStreamJob
		next  *replicationGRPCStreamJob
		bytes int
	}{
		{name: "metadata", first: newJob("source-a", "one", 1), next: newJob("source-b", "two", 1), bytes: 1024},
		{name: "bytes", first: newJob("source", "one", 8), next: newJob("source", "two", 8), bytes: 25},
	} {
		t.Run(test.name, func(t *testing.T) {
			target := &replicationGRPCStreamTarget{
				ctx: context.Background(), jobs: make(chan *replicationGRPCStreamJob, 1),
				batchMaxCommands: 4, batchMaxBytes: test.bytes,
			}
			target.jobs <- test.next
			flight, carry, err := target.collectFlight(test.first)
			if err != nil {
				t.Fatalf("collectFlight() error = %v", err)
			}
			if carry != test.next {
				t.Fatalf("collectFlight() carry = %p, want next job %p", carry, test.next)
			}
			if len(flight.jobs) != 1 || flight.jobs[0] != test.first || len(flight.request.GetKeys()) != 1 {
				t.Fatalf("collectFlight() = %#v, want first job only", flight)
			}
		})
	}
}

func TestReplicationGRPCStreamLivePipelinesBoundedAckWindow(t *testing.T) {
	listener := bufconn.Listen(testGRPCBufferSize)
	probe := &pipeliningReplicationGRPCServer{pipelined: make(chan struct{}), release: make(chan struct{})}
	server := grpc.NewServer()
	hatriecachev1.RegisterCacheServiceServer(server, probe)
	go func() {
		if err := server.Serve(listener); err != nil && err != grpc.ErrServerStopped {
			t.Errorf("grpc Serve() error = %v", err)
		}
	}()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})

	topology, err := NewTopologyStore(ClusterTopology{
		Version: 1,
		Self:    "node-a",
		Nodes: []TopologyNode{
			{ID: "node-a", Address: "http://node-a"},
			{ID: "node-b", Address: "http://node-b", GRPCAddress: "bufnet"},
		},
		Shards: []TopologyShard{{ID: 0, Primary: "node-a", Replicas: []string{"node-b"}}},
	})
	if err != nil {
		t.Fatalf("NewTopologyStore() error = %v", err)
	}
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:                     "node-a",
		Topology:                 topology,
		Election:                 NewElectionStore(topology, ElectionOptions{}),
		Transport:                ReplicationTransportGRPCStream,
		GRPCStreamWindow:         8,
		GRPCLiveBatchMaxCommands: 1,
		DisableHTTPFallback:      true,
		GRPCDialOptions: []grpc.DialOption{
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	})
	t.Cleanup(replicator.Close)
	sourceTrie := newTestTrie(t)
	sourceTrie.UpsertString("warmup", "value")
	warmup := replicator.ReplicateCommand(context.Background(), sourceTrie,
		CacheCommandRequest{Command: "SETSTR", Key: "warmup", Value: "value"}, CacheCommandResponse{OK: true})
	if len(warmup.Targets) != 1 || !warmup.Targets[0].OK {
		t.Fatalf("warmup replication = %#v, want success", warmup)
	}

	const commands = 8
	start := make(chan struct{})
	results := make(chan ReplicationResult, commands)
	var callers sync.WaitGroup
	callers.Add(commands)
	for idx := 0; idx < commands; idx++ {
		key := fmt.Sprintf("pipeline:%02d", idx)
		sourceTrie.UpsertString(key, "value")
		go func(key string) {
			defer callers.Done()
			<-start
			results <- replicator.ReplicateCommand(context.Background(), sourceTrie,
				CacheCommandRequest{Command: "SETSTR", Key: key, Value: "value"}, CacheCommandResponse{OK: true})
		}(key)
	}
	close(start)
	select {
	case <-probe.pipelined:
	case <-time.After(2 * time.Second):
		t.Fatal("server did not receive a second live batch before acknowledging the first")
	}
	close(probe.release)
	callers.Wait()
	close(results)
	for result := range results {
		if len(result.Targets) != 1 || !result.Targets[0].OK {
			t.Fatalf("pipelined replication result = %#v, want success", result)
		}
	}
}

func TestCacheGRPCServerReplicationReportsNotConfigured(t *testing.T) {
	ht := newTestTrie(t)
	client, stop := newTestGRPCClient(t, ht, CacheGRPCOptions{})
	defer stop()

	replication, err := client.Replication(context.Background(), &hatriecachev1.ReplicationRequest{})
	if err != nil {
		t.Fatalf("Replication() error = %v", err)
	}
	if !replication.GetSkipped() || replication.GetReason() != "replication is not configured" {
		t.Fatalf("replication status = %#v, want not configured skip", replication)
	}
	if replication.GetHealth() != "disabled" || replication.GetHealthScore() != 0 {
		t.Fatalf("replication health = %s/%d, want disabled 0", replication.GetHealth(), replication.GetHealthScore())
	}

	replication, err = client.Replication(context.Background(), &hatriecachev1.ReplicationRequest{
		Sync:   true,
		Prefix: "session:",
	})
	if err != nil {
		t.Fatalf("Replication(sync) error = %v", err)
	}
	if !replication.GetSkipped() || replication.GetCommand() != "SYNC" || replication.GetKey() != "session:" || replication.GetReason() != "replication is not configured" {
		t.Fatalf("replication sync status = %#v, want not configured sync skip", replication)
	}
	if replication.GetHealth() != "disabled" || replication.GetHealthScore() != 0 {
		t.Fatalf("replication sync health = %s/%d, want disabled 0", replication.GetHealth(), replication.GetHealthScore())
	}
}

func TestCacheGRPCServerSyncsReplication(t *testing.T) {
	ht := newTestTrie(t)
	ht.UpsertString("session:1", "value")
	ht.UpsertString("other:1", "ignored")
	requests := make(chan CacheCommandRequest, 1)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/commands" {
			t.Fatalf("path = %s, want /api/commands", r.URL.Path)
		}
		request := mustDecodeReplicationTestCommand(t, w, r)
		if rejectReplicationDigestTestCommand(t, w, r, request) {
			return
		}
		requests <- request
		writeJSON(w, CacheCommandResponse{OK: true, Message: "replicated"})
	}))
	defer target.Close()

	topology := replicationTestTopology(t, target.URL)
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:     "node-a",
		Topology: topology,
		Election: election,
		Client:   target.Client(),
	})
	client, stop := newTestGRPCClient(t, ht, CacheGRPCOptions{
		NodeName:   "node-a",
		Election:   election,
		Replicator: replicator,
	})
	defer stop()

	replication, err := client.Replication(context.Background(), &hatriecachev1.ReplicationRequest{
		Sync:   true,
		Prefix: "session:",
	})
	if err != nil {
		t.Fatalf("Replication(sync) error = %v", err)
	}
	if replication.GetSkipped() || replication.GetCommand() != "SYNC" || replication.GetKey() != "session:" || replication.GetEntries() != 1 {
		t.Fatalf("replication sync = %#v, want one synced session entry", replication)
	}
	if len(replication.GetTargets()) != 1 || !replication.GetTargets()[0].GetOk() || replication.GetTargets()[0].GetKey() != "session:1" {
		t.Fatalf("replication sync targets = %#v, want one ok session:1 target", replication.GetTargets())
	}
	select {
	case request := <-requests:
		if !isTypedReplicationSetPayload(request, "session:1") {
			t.Fatalf("replication sync request = %#v, want typed binary snapshot", request)
		}
	default:
		t.Fatal("gRPC replication sync did not reach remote target")
	}
}

func TestCacheGRPCServerReportsAsyncReplicationQueue(t *testing.T) {
	release := make(chan struct{})
	requests := make(chan CacheCommandRequest, 1)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request := mustDecodeReplicationTestCommand(t, w, r)
		requests <- request
		<-release
		writeJSON(w, CacheCommandResponse{OK: true, Message: "replicated"})
	}))
	t.Cleanup(target.Close)

	topology := replicationTestTopology(t, target.URL)
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:           "node-a",
		Topology:       topology,
		Election:       election,
		Client:         target.Client(),
		AsyncQueueSize: 2,
	})
	t.Cleanup(replicator.Close)
	t.Cleanup(func() {
		close(release)
	})

	ht := newTestTrie(t)
	client, stop := newTestGRPCClient(t, ht, CacheGRPCOptions{
		NodeName:   "node-a",
		Election:   election,
		Replicator: replicator,
	})
	defer stop()

	response, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "SETSTR",
		Key:     "session:1",
		Value:   "value",
	})
	if err != nil {
		t.Fatalf("Command(SETSTR) error = %v", err)
	}
	if !response.GetOk() {
		t.Fatalf("SETSTR response = %#v, want ok", response)
	}

	replication, err := client.Replication(context.Background(), &hatriecachev1.ReplicationRequest{})
	if err != nil {
		t.Fatalf("Replication() error = %v", err)
	}
	queue := replication.GetQueue()
	if !replication.GetQueued() || queue == nil || !queue.GetEnabled() || queue.GetCapacity() != 2 || queue.GetEnqueued() != 1 {
		t.Fatalf("async replication status = %#v, want queued status with queue stats", replication)
	}
	if replication.GetHealth() == "" || replication.GetHealthScore() <= 0 || replication.GetHealthScore() > 100 {
		t.Fatalf("async replication health = %s/%d, want populated score", replication.GetHealth(), replication.GetHealthScore())
	}
	select {
	case request := <-requests:
		if !isTypedReplicationSetPayload(request, "session:1") {
			t.Fatalf("async replication request = %#v, want typed binary snapshot", request)
		}
	case <-time.After(time.Second):
		t.Fatal("async replication request did not reach target")
	}
}

func TestCacheGRPCServerTopologyAndElection(t *testing.T) {
	topology := replicationTestTopology(t, "http://127.0.0.1:2")
	election := NewElectionStore(topology, ElectionOptions{})
	client, stop := newTestGRPCClient(t, newTestTrie(t), CacheGRPCOptions{
		NodeName: "node-a",
		Topology: topology,
		Election: election,
	})
	defer stop()

	topologyResp, err := client.Topology(context.Background(), &hatriecachev1.TopologyRequest{})
	if err != nil {
		t.Fatalf("Topology() error = %v", err)
	}
	if !topologyResp.GetOk() || topologyResp.GetTopology().GetSelf() != "node-a" || len(topologyResp.GetTopology().GetNodes()) != 2 {
		t.Fatalf("Topology() = %#v, want node-a topology with two nodes", topologyResp)
	}

	routeResp, err := client.Topology(context.Background(), &hatriecachev1.TopologyRequest{Key: "session:1"})
	if err != nil {
		t.Fatalf("Topology(key) error = %v", err)
	}
	route := routeResp.GetRoute()
	if !routeResp.GetOk() || route.GetKey() != "session:1" || route.GetShard().GetPrimary() != "node-a" || !reflect.DeepEqual(route.GetOwners(), []string{"node-a", "node-b"}) {
		t.Fatalf("Topology(key) = %#v, want node-a/node-b route", routeResp)
	}

	electionResp, err := client.Election(context.Background(), &hatriecachev1.ElectionRequest{})
	if err != nil {
		t.Fatalf("Election() error = %v", err)
	}
	if !electionResp.GetOk() || len(electionResp.GetStatus().GetLeaders()) != 1 || electionResp.GetStatus().GetLeaders()[0].GetLeader() != "node-a" {
		t.Fatalf("Election() = %#v, want node-a leader", electionResp)
	}

	offline := false
	updatedElection, err := client.UpdateElection(context.Background(), &hatriecachev1.UpdateElectionRequest{
		Node:   "node-a",
		Online: &offline,
	})
	if err != nil {
		t.Fatalf("UpdateElection(offline) error = %v", err)
	}
	if !updatedElection.GetOk() || updatedElection.GetStatus().GetLeaders()[0].GetLeader() != "node-b" {
		t.Fatalf("UpdateElection(offline) = %#v, want node-b leader", updatedElection)
	}

	keyElection, err := client.Election(context.Background(), &hatriecachev1.ElectionRequest{Key: "session:1"})
	if err != nil {
		t.Fatalf("Election(key) error = %v", err)
	}
	if !keyElection.GetOk() || keyElection.GetRoute().GetLeader().GetLeader() != "node-b" || keyElection.GetRoute().GetRoute().GetKey() != "session:1" {
		t.Fatalf("Election(key) = %#v, want session route with node-b leader", keyElection)
	}

	updatedTopology, err := client.UpdateTopology(context.Background(), &hatriecachev1.UpdateTopologyRequest{
		Topology: &hatriecachev1.ClusterTopology{
			Version: 1,
			Mode:    TopologyModeFullReplica,
			Self:    "node-b",
			Nodes: []*hatriecachev1.TopologyNode{
				{Id: "node-a", Address: "http://127.0.0.1:1"},
				{Id: "node-b", Address: "http://127.0.0.1:2"},
			},
		},
	})
	if err != nil {
		t.Fatalf("UpdateTopology() error = %v", err)
	}
	if !updatedTopology.GetOk() || updatedTopology.GetTopology().GetMode() != TopologyModeFullReplica || updatedTopology.GetTopology().GetSelf() != "node-b" {
		t.Fatalf("UpdateTopology() = %#v, want full-replica topology for node-b", updatedTopology)
	}

	invalidTopology, err := client.UpdateTopology(context.Background(), &hatriecachev1.UpdateTopologyRequest{
		Topology: &hatriecachev1.ClusterTopology{Version: 1},
	})
	if err != nil {
		t.Fatalf("UpdateTopology(invalid) error = %v", err)
	}
	if invalidTopology.GetOk() || !strings.Contains(invalidTopology.GetMessage(), "requires at least one node") {
		t.Fatalf("UpdateTopology(invalid) = %#v, want validation error", invalidTopology)
	}
}

func TestCacheGRPCServerTopologyPreservesBucketRangesAndShardReplicas(t *testing.T) {
	topology, err := NewTopologyStore(SingleNodeTopology("node-a", "http://127.0.0.1:1"))
	if err != nil {
		t.Fatalf("NewTopologyStore() error = %v", err)
	}
	client, stop := newTestGRPCClient(t, newTestTrie(t), CacheGRPCOptions{
		Topology: topology,
	})
	defer stop()

	updated, err := client.UpdateTopology(context.Background(), &hatriecachev1.UpdateTopologyRequest{
		Topology: &hatriecachev1.ClusterTopology{
			Version:     1,
			Mode:        TopologyModeSharded,
			BucketCount: 16,
			Self:        "node-a",
			BucketRanges: []*hatriecachev1.TopologyBucketRange{
				{Start: 8, End: 15, Shard: 1},
				{Start: 0, End: 7, Shard: 0},
			},
			Nodes: []*hatriecachev1.TopologyNode{
				{Id: "node-a", Address: "http://127.0.0.1:1", Role: "primary"},
				{Id: "node-b", Address: "http://127.0.0.1:2", Role: "replica"},
				{Id: "node-c", Address: "http://127.0.0.1:3", Role: "replica"},
			},
			Shards: []*hatriecachev1.TopologyShard{
				{Id: 1, Primary: "node-a", Replicas: []string{"node-c", "node-b"}},
				{Id: 0, Primary: "node-b", Replicas: []string{"node-c", "node-a"}},
			},
		},
	})
	if err != nil {
		t.Fatalf("UpdateTopology(bucketed) error = %v", err)
	}
	got := updated.GetTopology()
	if !updated.GetOk() || got.GetBucketCount() != 16 || got.GetMode() != TopologyModeSharded {
		t.Fatalf("UpdateTopology(bucketed) = %#v, want sharded bucketed topology", updated)
	}
	if gotRanges := protoBucketRanges(got.GetBucketRanges()); !reflect.DeepEqual(gotRanges, []TopologyBucketRange{
		{Start: 0, End: 7, Shard: 0},
		{Start: 8, End: 15, Shard: 1},
	}) {
		t.Fatalf("bucket ranges = %#v, want sorted compact ranges", gotRanges)
	}
	if len(got.GetShards()) != 2 {
		t.Fatalf("shards = %d, want 2", len(got.GetShards()))
	}
	if shard := got.GetShards()[0]; shard.GetId() != 0 || shard.GetPrimary() != "node-b" || !reflect.DeepEqual(shard.GetReplicas(), []string{"node-a", "node-c"}) {
		t.Fatalf("shard[0] = %#v, want shard 0 primary node-b replicas node-a,node-c", shard)
	}
	if shard := got.GetShards()[1]; shard.GetId() != 1 || shard.GetPrimary() != "node-a" || !reflect.DeepEqual(shard.GetReplicas(), []string{"node-b", "node-c"}) {
		t.Fatalf("shard[1] = %#v, want shard 1 primary node-a replicas node-b,node-c", shard)
	}

	key := "session:bucketed"
	bucket := hashKeyToBucket(key, got.GetBucketCount())
	wantShardID := uint32(0)
	wantOwners := []string{"node-b", "node-a", "node-c"}
	if bucket >= 8 {
		wantShardID = 1
		wantOwners = []string{"node-a", "node-b", "node-c"}
	}
	routed, err := client.Topology(context.Background(), &hatriecachev1.TopologyRequest{Key: key})
	if err != nil {
		t.Fatalf("Topology(bucketed key) error = %v", err)
	}
	route := routed.GetRoute()
	if !routed.GetOk() || route == nil || route.Bucket == nil || route.GetBucket() != bucket || route.GetShard().GetId() != wantShardID || !reflect.DeepEqual(route.GetOwners(), wantOwners) {
		t.Fatalf("Topology(bucketed key) = %#v, want bucket %d shard %d owners %#v", routed, bucket, wantShardID, wantOwners)
	}
}

func protoBucketRanges(ranges []*hatriecachev1.TopologyBucketRange) []TopologyBucketRange {
	out := make([]TopologyBucketRange, 0, len(ranges))
	for _, bucketRange := range ranges {
		if bucketRange == nil {
			out = append(out, TopologyBucketRange{})
			continue
		}
		out = append(out, TopologyBucketRange{
			Start: bucketRange.GetStart(),
			End:   bucketRange.GetEnd(),
			Shard: bucketRange.GetShard(),
		})
	}
	return out
}

func TestCacheGRPCServerTopologyAndElectionRequireStores(t *testing.T) {
	client, stop := newTestGRPCClient(t, newTestTrie(t), CacheGRPCOptions{})
	defer stop()

	topology, err := client.Topology(context.Background(), &hatriecachev1.TopologyRequest{})
	if err != nil {
		t.Fatalf("Topology() error = %v", err)
	}
	if topology.GetOk() || !strings.Contains(topology.GetMessage(), "topology store is not configured") {
		t.Fatalf("Topology() = %#v, want missing topology store response", topology)
	}

	updateTopology, err := client.UpdateTopology(context.Background(), &hatriecachev1.UpdateTopologyRequest{})
	if err != nil {
		t.Fatalf("UpdateTopology() error = %v", err)
	}
	if updateTopology.GetOk() || !strings.Contains(updateTopology.GetMessage(), "topology store is not configured") {
		t.Fatalf("UpdateTopology() = %#v, want missing topology store response", updateTopology)
	}

	election, err := client.Election(context.Background(), &hatriecachev1.ElectionRequest{})
	if err != nil {
		t.Fatalf("Election() error = %v", err)
	}
	if election.GetOk() || !strings.Contains(election.GetMessage(), "election store is not configured") {
		t.Fatalf("Election() = %#v, want missing election store response", election)
	}

	updateElection, err := client.UpdateElection(context.Background(), &hatriecachev1.UpdateElectionRequest{})
	if err != nil {
		t.Fatalf("UpdateElection() error = %v", err)
	}
	if updateElection.GetOk() || !strings.Contains(updateElection.GetMessage(), "election store is not configured") {
		t.Fatalf("UpdateElection() = %#v, want missing election store response", updateElection)
	}
}

func TestCacheGRPCServerSnapshotRequiresCallback(t *testing.T) {
	ht := newTestTrie(t)
	client, stop := newTestGRPCClient(t, ht, CacheGRPCOptions{})
	defer stop()

	resp, err := client.Snapshot(context.Background(), &hatriecachev1.SnapshotRequest{})
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	if resp.GetOk() {
		t.Fatalf("Snapshot response = %#v, want not ok", resp)
	}
}

func TestCacheGRPCServerHonorsCanceledContexts(t *testing.T) {
	ht := newTestTrie(t)
	server := NewCacheGRPCServer(ht, CacheGRPCOptions{
		Snapshot: func() error {
			t.Fatal("snapshot callback should not run for canceled context")
			return nil
		},
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := server.Health(ctx, &hatriecachev1.HealthRequest{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Health(canceled) error = %v, want context.Canceled", err)
	}
	if _, err := server.Stats(ctx, &hatriecachev1.StatsRequest{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Stats(canceled) error = %v, want context.Canceled", err)
	}
	if _, err := server.Entries(ctx, &hatriecachev1.EntriesRequest{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Entries(canceled) error = %v, want context.Canceled", err)
	}
	if _, err := server.Command(ctx, &hatriecachev1.CommandRequest{Command: "SETSTR", Key: "name", Value: "ivi"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Command(canceled) error = %v, want context.Canceled", err)
	}
	if got := ht.GetString("name"); got != "" {
		t.Fatalf("Command(canceled) mutated trie: name=%q", got)
	}
	if _, err := server.Snapshot(ctx, &hatriecachev1.SnapshotRequest{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Snapshot(canceled) error = %v, want context.Canceled", err)
	}
	if _, err := server.Replication(ctx, &hatriecachev1.ReplicationRequest{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Replication(canceled) error = %v, want context.Canceled", err)
	}
	if _, err := server.Topology(ctx, &hatriecachev1.TopologyRequest{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Topology(canceled) error = %v, want context.Canceled", err)
	}
	if _, err := server.UpdateTopology(ctx, &hatriecachev1.UpdateTopologyRequest{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("UpdateTopology(canceled) error = %v, want context.Canceled", err)
	}
	if _, err := server.Election(ctx, &hatriecachev1.ElectionRequest{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Election(canceled) error = %v, want context.Canceled", err)
	}
	if _, err := server.UpdateElection(ctx, &hatriecachev1.UpdateElectionRequest{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("UpdateElection(canceled) error = %v, want context.Canceled", err)
	}
}

func TestCacheGRPCServerHandlesNilContextAndRequests(t *testing.T) {
	ht := newTestTrie(t)
	server := NewCacheGRPCServer(ht, CacheGRPCOptions{})

	if _, err := server.Health(nil, nil); err != nil {
		t.Fatalf("Health(nil) error = %v", err)
	}
	if _, err := server.Stats(nil, nil); err != nil {
		t.Fatalf("Stats(nil) error = %v", err)
	}
	entries, err := server.Entries(nil, nil)
	if err != nil {
		t.Fatalf("Entries(nil) error = %v", err)
	}
	if len(entries.GetEntries()) != 0 {
		t.Fatalf("Entries(nil) = %#v, want empty response", entries)
	}
	command, err := server.Command(nil, nil)
	if err != nil {
		t.Fatalf("Command(nil) error = %v", err)
	}
	if command.GetOk() || !strings.Contains(command.GetMessage(), "command is required") {
		t.Fatalf("Command(nil) = %#v, want validation response", command)
	}
	snapshot, err := server.Snapshot(nil, nil)
	if err != nil {
		t.Fatalf("Snapshot(nil) error = %v", err)
	}
	if snapshot.GetOk() || !strings.Contains(snapshot.GetMessage(), "snapshot path is not configured") {
		t.Fatalf("Snapshot(nil) = %#v, want missing snapshot response", snapshot)
	}
	replication, err := server.Replication(nil, nil)
	if err != nil {
		t.Fatalf("Replication(nil) error = %v", err)
	}
	if !replication.GetSkipped() || !strings.Contains(replication.GetReason(), "replication is not configured") {
		t.Fatalf("Replication(nil) = %#v, want not configured response", replication)
	}
	if replication.GetHealth() != "disabled" || replication.GetHealthScore() != 0 {
		t.Fatalf("Replication(nil) health = %s/%d, want disabled 0", replication.GetHealth(), replication.GetHealthScore())
	}
	topology, err := server.Topology(nil, nil)
	if err != nil {
		t.Fatalf("Topology(nil) error = %v", err)
	}
	if topology.GetOk() || !strings.Contains(topology.GetMessage(), "topology store is not configured") {
		t.Fatalf("Topology(nil) = %#v, want missing topology response", topology)
	}
	updateTopology, err := server.UpdateTopology(nil, nil)
	if err != nil {
		t.Fatalf("UpdateTopology(nil) error = %v", err)
	}
	if updateTopology.GetOk() || !strings.Contains(updateTopology.GetMessage(), "topology store is not configured") {
		t.Fatalf("UpdateTopology(nil) = %#v, want missing topology response", updateTopology)
	}
	election, err := server.Election(nil, nil)
	if err != nil {
		t.Fatalf("Election(nil) error = %v", err)
	}
	if election.GetOk() || !strings.Contains(election.GetMessage(), "election store is not configured") {
		t.Fatalf("Election(nil) = %#v, want missing election response", election)
	}
	updateElection, err := server.UpdateElection(nil, nil)
	if err != nil {
		t.Fatalf("UpdateElection(nil) error = %v", err)
	}
	if updateElection.GetOk() || !strings.Contains(updateElection.GetMessage(), "election store is not configured") {
		t.Fatalf("UpdateElection(nil) = %#v, want missing election response", updateElection)
	}
}

func TestCacheGRPCServerRejectsNilTrie(t *testing.T) {
	server := NewCacheGRPCServer(nil, CacheGRPCOptions{})
	for _, tt := range []struct {
		name string
		call func() error
	}{
		{
			name: "health",
			call: func() error {
				_, err := server.Health(context.Background(), &hatriecachev1.HealthRequest{})
				return err
			},
		},
		{
			name: "stats",
			call: func() error {
				_, err := server.Stats(context.Background(), &hatriecachev1.StatsRequest{})
				return err
			},
		},
		{
			name: "entries",
			call: func() error {
				_, err := server.Entries(context.Background(), &hatriecachev1.EntriesRequest{})
				return err
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.call()
			if status.Code(err) != codes.Unavailable || !strings.Contains(err.Error(), "trie is not configured") {
				t.Fatalf("%s error = %v, want Unavailable trie error", tt.name, err)
			}
		})
	}

	command, err := server.Command(context.Background(), &hatriecachev1.CommandRequest{Command: "GET", Key: "name"})
	if err != nil {
		t.Fatalf("Command(nil trie) error = %v", err)
	}
	if command.GetOk() || command.GetMessage() != "trie is not configured" {
		t.Fatalf("Command(nil trie) = %#v, want command error response", command)
	}
}

func grpcEntryKeys(entries []*hatriecachev1.Entry) []string {
	keys := make([]string, len(entries))
	for idx, entry := range entries {
		keys[idx] = entry.GetKey()
	}
	return keys
}

func newTestGRPCClient(t *testing.T, ht *HatTrie, options CacheGRPCOptions) (hatriecachev1.CacheServiceClient, func()) {
	t.Helper()

	listener := bufconn.Listen(testGRPCBufferSize)
	server := grpc.NewServer()
	RegisterCacheGRPCServer(server, NewCacheGRPCServer(ht, options))
	go func() {
		if err := server.Serve(listener); err != nil && err != grpc.ErrServerStopped {
			t.Errorf("grpc Serve() error = %v", err)
		}
	}()

	ctx := context.Background()
	conn, err := grpc.DialContext(ctx, "bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("DialContext() error = %v", err)
	}

	stop := func() {
		if err := conn.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
		server.Stop()
		if err := listener.Close(); err != nil {
			t.Fatalf("listener Close() error = %v", err)
		}
	}
	return hatriecachev1.NewCacheServiceClient(conn), stop
}
