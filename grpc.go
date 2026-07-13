package hatriecache

import (
	"context"
	"os"
	"runtime"
	"time"

	"google.golang.org/grpc"
	hatriecachev1 "hatrie_cache/internal/gen/hatriecache/v1"
)

type CacheGRPCOptions struct {
	NodeName string
	StartAt  time.Time
	Snapshot func() error
	Journal  *CommandJournal
}

type CacheGRPCServer struct {
	hatriecachev1.UnimplementedCacheServiceServer
	trie    *HatTrie
	options CacheGRPCOptions
}

func NewCacheGRPCServer(trie *HatTrie, options CacheGRPCOptions) *CacheGRPCServer {
	if options.StartAt.IsZero() {
		options.StartAt = time.Now()
	}
	if options.NodeName == "" {
		if hostname, err := os.Hostname(); err == nil && hostname != "" {
			options.NodeName = hostname
		} else {
			options.NodeName = "local"
		}
	}
	return &CacheGRPCServer{trie: trie, options: options}
}

func RegisterCacheGRPCServer(registrar grpc.ServiceRegistrar, server *CacheGRPCServer) {
	hatriecachev1.RegisterCacheServiceServer(registrar, server)
}

func (server *CacheGRPCServer) Health(ctx context.Context, _ *hatriecachev1.HealthRequest) (*hatriecachev1.HealthResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	return &hatriecachev1.HealthResponse{
		Status:          "online",
		Node:            server.options.NodeName,
		UptimeSeconds:   int64(time.Since(server.options.StartAt).Seconds()),
		MemoryBytes:     mem.Alloc,
		DiskSpillBytes:  server.trie.diskSpillBytes(),
		CleanersRunning: 0,
	}, nil
}

func (server *CacheGRPCServer) Stats(ctx context.Context, _ *hatriecachev1.StatsRequest) (*hatriecachev1.StatsResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	stats := server.trie.Stats()
	return &hatriecachev1.StatsResponse{
		Reads:             stats.Reads,
		Hits:              stats.Hits,
		Misses:            stats.Misses,
		Writes:            stats.Writes,
		Deletes:           stats.Deletes,
		Expirations:       stats.Expirations,
		LastHitUnixNano:   unixNanoOrZero(stats.LastHit),
		LastMissUnixNano:  unixNanoOrZero(stats.LastMiss),
		LastWriteUnixNano: unixNanoOrZero(stats.LastWrite),
		HitRate:           stats.HitRate,
		CumulativeHitRate: stats.CumulativeHitRate,
	}, nil
}

func (server *CacheGRPCServer) Entries(ctx context.Context, request *hatriecachev1.EntriesRequest) (*hatriecachev1.EntriesResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	entries := server.trie.monitoringEntries(request.GetPrefix())
	out := make([]*hatriecachev1.Entry, 0, len(entries))
	for _, entry := range entries {
		out = append(out, grpcEntry(entry))
	}
	return &hatriecachev1.EntriesResponse{Entries: out}, nil
}

func (server *CacheGRPCServer) Command(ctx context.Context, request *hatriecachev1.CommandRequest) (*hatriecachev1.CommandResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	command := cacheCommandRequestFromProto(request)
	var response CacheCommandResponse
	if server.options.Journal != nil {
		response = server.options.Journal.ExecuteCommand(server.trie, command)
	} else {
		response = server.trie.ExecuteCommand(command)
	}
	return grpcCommandResponse(response), nil
}

func (server *CacheGRPCServer) Snapshot(ctx context.Context, _ *hatriecachev1.SnapshotRequest) (*hatriecachev1.CommandResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if server.options.Snapshot == nil {
		return grpcCommandResponse(commandError("snapshot path is not configured")), nil
	}
	if err := server.options.Snapshot(); err != nil {
		return grpcCommandResponse(commandError(err.Error())), nil
	}
	return grpcCommandResponse(CacheCommandResponse{OK: true, Message: "snapshot saved"}), nil
}

func grpcEntry(entry MonitoringEntry) *hatriecachev1.Entry {
	out := &hatriecachev1.Entry{
		Key:          entry.Key,
		Type:         entry.Type,
		OnDisk:       entry.OnDisk,
		SizeBytes:    entry.SizeBytes,
		ValuePreview: entry.ValuePreview,
	}
	if entry.TTLMillis != nil {
		out.TtlMillis = entry.TTLMillis
	}
	return out
}

func cacheCommandRequestFromProto(request *hatriecachev1.CommandRequest) CacheCommandRequest {
	out := CacheCommandRequest{
		Command: request.GetCommand(),
		Key:     request.GetKey(),
		Value:   request.GetValue(),
		Subkey:  request.GetSubkey(),
	}
	if request.TtlSeconds != nil {
		ttl := request.GetTtlSeconds()
		out.TTLSeconds = &ttl
	}
	if request.UnixSeconds != nil {
		unixSeconds := request.GetUnixSeconds()
		out.UnixSeconds = &unixSeconds
	}
	if request.Priority != nil {
		priority := request.GetPriority()
		out.Priority = &priority
	}
	if len(request.Values) > 0 {
		out.Values = make(Slice, len(request.Values))
		for i, value := range request.Values {
			out.Values[i] = value
		}
	}
	if len(request.Pairs) > 0 {
		out.Pairs = make(Map, len(request.Pairs))
		for key, value := range request.Pairs {
			out.Pairs[key] = value
		}
	}
	return out
}

func grpcCommandResponse(response CacheCommandResponse) *hatriecachev1.CommandResponse {
	return &hatriecachev1.CommandResponse{
		Ok:      response.OK,
		Message: response.Message,
		Value:   response.Value,
	}
}

func unixNanoOrZero(value time.Time) int64 {
	if value.IsZero() {
		return 0
	}
	return value.UnixNano()
}
