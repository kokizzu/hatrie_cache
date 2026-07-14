package hatriecache

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	hatriecachev1 "hatrie_cache/internal/gen/hatriecache/v1"
)

const testGRPCBufferSize = 1024 * 1024

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
		var request CacheCommandRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatalf("Decode() error = %v", err)
		}
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
		if request.Command != "INTERNALSET" || request.Key != "session:1" || request.Value == "" {
			t.Fatalf("replicated request = %#v, want INTERNALSET snapshot", request)
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
		var request CacheCommandRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatalf("Decode() error = %v", err)
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
		if request.Command != "INTERNALSET" || request.Key != "session:1" || request.Value == "" {
			t.Fatalf("replication sync request = %#v, want INTERNALSET snapshot", request)
		}
	default:
		t.Fatal("gRPC replication sync did not reach remote target")
	}
}

func TestCacheGRPCServerReportsAsyncReplicationQueue(t *testing.T) {
	release := make(chan struct{})
	requests := make(chan CacheCommandRequest, 1)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request CacheCommandRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatalf("Decode() error = %v", err)
		}
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
	select {
	case request := <-requests:
		if request.Command != "INTERNALSET" || request.Key != "session:1" || request.Value == "" {
			t.Fatalf("async replication request = %#v, want INTERNALSET snapshot", request)
		}
	default:
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
