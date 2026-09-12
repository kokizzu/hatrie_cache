package hatCache

import (
	"context"
	"errors"
	"io"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	hatriecachev1 "hatrie_cache/internal/gen/hatriecache/v1"
)

const (
	defaultCommandStreamWorkers = 1
	maxCommandStreamWorkers     = 64
)

func normalizedCommandStreamWorkers(workers int) int {
	if workers < 1 {
		return defaultCommandStreamWorkers
	}
	if workers > maxCommandStreamWorkers {
		return maxCommandStreamWorkers
	}
	return workers
}

func (server *CacheGRPCServer) executeGRPCCommandStreamRequest(ctx context.Context, request *hatriecachev1.CommandRequest) (*hatriecachev1.CommandResponse, error) {
	response, err := server.executeGRPCCommand(ctx, request, "/hatriecache.v1.CacheService/CommandStream")
	if err != nil {
		return nil, err
	}
	response.RequestId = request.GetRequestId()
	return response, nil
}

type commandStreamResult struct {
	requestID uint64
	response  *hatriecachev1.CommandResponse
	err       error
}

func (server *CacheGRPCServer) commandStreamMultiplexed(ctx context.Context, stream hatriecachev1.CacheService_CommandStreamServer, first *hatriecachev1.CommandRequest) error {
	workers := normalizedCommandStreamWorkers(server.options.CommandStreamWorkers)
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	incoming := make(chan *hatriecachev1.CommandRequest, workers)
	jobs := make(chan *hatriecachev1.CommandRequest, workers)
	results := make(chan commandStreamResult, workers)
	recvErr := make(chan error, 1)

	var recvWG sync.WaitGroup
	recvWG.Add(1)
	go func() {
		defer recvWG.Done()
		defer close(incoming)
		request := first
		select {
		case incoming <- request:
		case <-streamCtx.Done():
			return
		}
		for {
			request = new(hatriecachev1.CommandRequest)
			err := stream.RecvMsg(request)
			if errors.Is(err, io.EOF) {
				recvErr <- nil
				return
			}
			if err != nil {
				recvErr <- err
				return
			}
			select {
			case incoming <- request:
			case <-streamCtx.Done():
				return
			}
		}
	}()

	var workerWG sync.WaitGroup
	workerWG.Add(workers)
	for worker := 0; worker < workers; worker++ {
		go func() {
			defer workerWG.Done()
			for request := range jobs {
				response, err := server.executeGRPCCommandStreamRequest(streamCtx, request)
				result := commandStreamResult{requestID: request.GetRequestId(), response: response, err: err}
				select {
				case results <- result:
				case <-streamCtx.Done():
					return
				}
			}
		}()
	}

	incomingOpen := true
	jobsClosed := false
	pending := 0
	seen := make(map[uint64]struct{}, workers)
	closeJobs := func() {
		if !jobsClosed {
			close(jobs)
			jobsClosed = true
		}
	}
	handleResult := func(result commandStreamResult) error {
		pending--
		delete(seen, result.requestID)
		if result.err != nil {
			return result.err
		}
		return stream.Send(result.response)
	}

	for incomingOpen || pending > 0 {
		if !incomingOpen || pending >= workers {
			select {
			case result := <-results:
				if err := handleResult(result); err != nil {
					cancel()
					closeJobs()
					workerWG.Wait()
					return err
				}
			case <-streamCtx.Done():
				closeJobs()
				workerWG.Wait()
				return streamCtx.Err()
			}
			continue
		}

		select {
		case request, ok := <-incoming:
			if !ok {
				incomingOpen = false
				var recvError error
				select {
				case recvError = <-recvErr:
				case <-streamCtx.Done():
					closeJobs()
					workerWG.Wait()
					return streamCtx.Err()
				}
				if recvError != nil {
					cancel()
					closeJobs()
					workerWG.Wait()
					return recvError
				}
				closeJobs()
				continue
			}
			requestID := request.GetRequestId()
			if requestID == 0 {
				cancel()
				closeJobs()
				workerWG.Wait()
				return status.Error(codes.InvalidArgument, "CommandStream request_id must be nonzero when multiplexing")
			}
			if _, exists := seen[requestID]; exists {
				cancel()
				closeJobs()
				workerWG.Wait()
				return status.Errorf(codes.InvalidArgument, "CommandStream request_id %d is already in flight", requestID)
			}
			seen[requestID] = struct{}{}
			jobs <- request
			pending++
		case result := <-results:
			if err := handleResult(result); err != nil {
				cancel()
				closeJobs()
				workerWG.Wait()
				return err
			}
		case <-streamCtx.Done():
			closeJobs()
			workerWG.Wait()
			return streamCtx.Err()
		}
	}

	workerWG.Wait()
	recvWG.Wait()
	return nil
}
