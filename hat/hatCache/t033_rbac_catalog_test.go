package hatCache

import (
	"context"
	"testing"

	"google.golang.org/grpc/metadata"
	"hatrie_cache/hat/hatAuth"
)

func t33HandlerRoleCatalog(t *testing.T) *hatAuth.RoleCatalog {
	t.Helper()
	catalog, err := hatAuth.NewRoleCatalog(hatAuth.RoleCatalogOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.CreateRole("admin", hatAuth.RoleSpec{Name: "reader", Owner: "admin"}); err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.CreateNamespace("admin", hatAuth.NamespaceSpec{Name: "tenant-eu:orders", Owner: "admin"}); err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.Grant("admin", hatAuth.RoleGrantSpec{
		Role: "reader",
		Rule: hatAuth.Rule{Commands: []string{"GETSTR"}, Namespaces: []string{"tenant-eu:orders"}, Objects: []string{"tenant-eu:orders"}},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.GrantRole("admin", "alice", "reader"); err != nil {
		t.Fatal(err)
	}
	return catalog
}

func TestT33MonitoringAndGRPCUseRoleCatalogAuthorization(t *testing.T) {
	catalog := t33HandlerRoleCatalog(t)
	handler := NewMonitoringHandler(newTestTrie(t), MonitoringOptions{RBACCatalog: catalog})
	request := CacheCommandRequest{Command: "GETSTR", Key: "tenant-eu:orders"}
	if !handler.authorizeCommand("alice", request) {
		t.Fatal("monitoring handler should honor role catalog grant")
	}
	if handler.authorizeCommand("alice", CacheCommandRequest{Command: "SETSTR", Key: request.Key}) {
		t.Fatal("monitoring handler should deny command outside catalog grant")
	}
	if handler.authorizeCommand("bob", request) {
		t.Fatal("monitoring handler should deny unassigned principal")
	}

	server := NewCacheGRPCServer(newTestTrie(t), CacheGRPCOptions{AuthToken: "alice", RBACCatalog: catalog})
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-hatrie-auth-token", "alice"))
	if !server.authorizeGRPCCommand(ctx, request) {
		t.Fatal("gRPC server should honor role catalog grant")
	}
	if server.authorizeGRPCCommand(ctx, CacheCommandRequest{Command: "SETSTR", Key: request.Key}) {
		t.Fatal("gRPC server should deny command outside catalog grant")
	}
}
