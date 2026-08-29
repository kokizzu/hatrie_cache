package hatPgWire

import "sync/atomic"

// ServerMetrics collects optional PostgreSQL wire server counters. It is safe
// to share across concurrent connections.
type ServerMetrics struct {
	connectionsTotal      atomic.Uint64
	activeConnections     atomic.Int64
	tlsConnectionsTotal   atomic.Uint64
	frontendMessagesTotal atomic.Uint64
	simpleQueriesTotal    atomic.Uint64
	extendedQueriesTotal  atomic.Uint64
	errorsTotal           atomic.Uint64
	cancelRequestsTotal   atomic.Uint64
}

// ServerMetricsSnapshot is a consistent-copy view of ServerMetrics counters.
type ServerMetricsSnapshot struct {
	ConnectionsTotal      uint64
	ActiveConnections     int64
	TLSConnectionsTotal   uint64
	FrontendMessagesTotal uint64
	SimpleQueriesTotal    uint64
	ExtendedQueriesTotal  uint64
	ErrorsTotal           uint64
	CancelRequestsTotal   uint64
}

// NewServerMetrics creates an empty optional PGWire metrics collector.
func NewServerMetrics() *ServerMetrics {
	return &ServerMetrics{}
}

// Snapshot returns the current PGWire counter values.
func (metrics *ServerMetrics) Snapshot() ServerMetricsSnapshot {
	if metrics == nil {
		return ServerMetricsSnapshot{}
	}
	return ServerMetricsSnapshot{
		ConnectionsTotal:      metrics.connectionsTotal.Load(),
		ActiveConnections:     metrics.activeConnections.Load(),
		TLSConnectionsTotal:   metrics.tlsConnectionsTotal.Load(),
		FrontendMessagesTotal: metrics.frontendMessagesTotal.Load(),
		SimpleQueriesTotal:    metrics.simpleQueriesTotal.Load(),
		ExtendedQueriesTotal:  metrics.extendedQueriesTotal.Load(),
		ErrorsTotal:           metrics.errorsTotal.Load(),
		CancelRequestsTotal:   metrics.cancelRequestsTotal.Load(),
	}
}

func (metrics *ServerMetrics) recordConnection(tls bool) {
	if metrics == nil {
		return
	}
	metrics.connectionsTotal.Add(1)
	metrics.activeConnections.Add(1)
	if tls {
		metrics.tlsConnectionsTotal.Add(1)
	}
}

func (metrics *ServerMetrics) recordConnectionClosed() {
	if metrics != nil {
		metrics.activeConnections.Add(-1)
	}
}

func (metrics *ServerMetrics) recordFrontendMessage() {
	if metrics != nil {
		metrics.frontendMessagesTotal.Add(1)
	}
}

func (metrics *ServerMetrics) recordSimpleQuery() {
	if metrics != nil {
		metrics.simpleQueriesTotal.Add(1)
	}
}

func (metrics *ServerMetrics) recordExtendedQuery() {
	if metrics != nil {
		metrics.extendedQueriesTotal.Add(1)
	}
}

func (metrics *ServerMetrics) recordError() {
	if metrics != nil {
		metrics.errorsTotal.Add(1)
	}
}

func (metrics *ServerMetrics) recordCancelRequest() {
	if metrics != nil {
		metrics.cancelRequestsTotal.Add(1)
	}
}
