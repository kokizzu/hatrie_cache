package hatCache

// LeaderFencingTokenPair is the reserved command-pair key carrying the
// topology generation used to fence a public leader write. Clients should
// obtain the current value from the authoritative topology before writing.
const LeaderFencingTokenPair = replicationMetaFencingToken

func stripLeaderFencingToken(request CacheCommandRequest) CacheCommandRequest {
	if len(request.Pairs) == 0 {
		return request
	}
	if _, ok := request.Pairs[LeaderFencingTokenPair]; !ok {
		return request
	}
	pairs := make(Map, len(request.Pairs)-1)
	for key, value := range request.Pairs {
		if key != LeaderFencingTokenPair {
			pairs[key] = value
		}
	}
	request.Pairs = pairs
	return request
}

func rejectStrictReplicationFencing(request CacheCommandRequest, options commandExecutionOptions) (CacheCommandResponse, bool) {
	if !options.EnforceLeaderFencing {
		return CacheCommandResponse{}, false
	}
	switch normalizedCommand(request.Command) {
	case "INTERNALSET", "INTERNALDEL", replicationBatchEnvelopeCommand, replicationSetBinaryCommand, replicationSetCompactCommand:
	default:
		return CacheCommandResponse{}, false
	}
	token, present, err := replicationFencingToken(request)
	if err != nil {
		return commandError("invalid replication fencing token"), true
	}
	if !present && options.inheritedReplicationFencingTokenSet {
		token = options.inheritedReplicationFencingToken
		present = true
	}
	if !present {
		return commandError("replication fencing token is required"), true
	}
	if options.Topology == nil {
		return commandError("replication fencing requires a topology store"), true
	}
	if token != options.Topology.FencingToken() {
		return commandError("replication fencing token mismatch"), true
	}
	return CacheCommandResponse{}, false
}
