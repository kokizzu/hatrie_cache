package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	hatriecache "hatrie_cache"
)

const serverShutdownTimeout = 5 * time.Second

type config struct {
	monitoringServer  bool
	monitoringAddr    string
	monitoringTLSCert string
	monitoringTLSKey  string
	monitoringWebDir  string
	snapshotPath      string
	snapshotInterval  time.Duration
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, stdout io.Writer, stderr io.Writer) error {
	cfg, err := parseConfig(args, stderr)
	if err != nil {
		return err
	}
	if !cfg.monitoringServer {
		fmt.Fprintln(stdout, "monitoring server disabled; pass -monitoring-server to start it")
		return nil
	}

	trie := hatriecache.CreateHatTrie()
	defer trie.Destroy()
	if err := loadSnapshotIfConfigured(trie, cfg.snapshotPath); err != nil {
		return err
	}
	if cfg.snapshotPath != "" {
		defer func() {
			if err := trie.SaveSnapshot(cfg.snapshotPath); err != nil {
				fmt.Fprintf(stderr, "save snapshot: %v\n", err)
			}
		}()
	}
	stopSnapshots := startSnapshotSaver(ctx, trie, cfg.snapshotPath, cfg.snapshotInterval, stderr)
	defer stopSnapshots()

	handler := hatriecache.NewMonitoringHandler(trie, hatriecache.MonitoringOptions{
		WebDir:   cfg.monitoringWebDir,
		Snapshot: snapshotCallback(trie, cfg.snapshotPath),
	}).Handler()
	server := &http.Server{
		Addr:      cfg.monitoringAddr,
		Handler:   handler,
		TLSConfig: monitoringTLSConfig(nil),
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- serveMonitoring(server, cfg)
	}()
	fmt.Fprintf(stdout, "monitoring server listening on %s\n", monitoringURL(cfg))

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), serverShutdownTimeout)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			return err
		}
		return nil
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

func parseConfig(args []string, output io.Writer) (config, error) {
	cfg := config{
		monitoringAddr:   "127.0.0.1:8080",
		monitoringWebDir: "svelte-mpa/dist",
	}
	flags := flag.NewFlagSet("hatrie-cache", flag.ContinueOnError)
	flags.SetOutput(output)
	flags.BoolVar(&cfg.monitoringServer, "monitoring-server", false, "run the grpc/http2/web monitoring server")
	flags.StringVar(&cfg.monitoringAddr, "monitoring-addr", cfg.monitoringAddr, "monitoring server listen address")
	flags.StringVar(&cfg.monitoringTLSCert, "monitoring-tls-cert", "", "TLS certificate path for HTTPS/HTTP2 monitoring")
	flags.StringVar(&cfg.monitoringTLSKey, "monitoring-tls-key", "", "TLS private key path for HTTPS/HTTP2 monitoring")
	flags.StringVar(&cfg.monitoringWebDir, "monitoring-web-dir", cfg.monitoringWebDir, "directory containing built web monitoring assets")
	flags.StringVar(&cfg.snapshotPath, "snapshot-path", "", "optional JSON snapshot path to load on startup and save on shutdown")
	flags.DurationVar(&cfg.snapshotInterval, "snapshot-interval", 0, "optional periodic snapshot interval")
	if err := flags.Parse(args); err != nil {
		return config{}, err
	}
	if (cfg.monitoringTLSCert == "") != (cfg.monitoringTLSKey == "") {
		return config{}, errors.New("monitoring TLS requires both -monitoring-tls-cert and -monitoring-tls-key")
	}
	return cfg, nil
}

func serveMonitoring(server *http.Server, cfg config) error {
	if cfg.monitoringTLSCert == "" {
		return server.ListenAndServe()
	}
	return server.ListenAndServeTLS(cfg.monitoringTLSCert, cfg.monitoringTLSKey)
}

func monitoringURL(cfg config) string {
	scheme := "http"
	if cfg.monitoringTLSCert != "" {
		scheme = "https"
	}
	return scheme + "://" + cfg.monitoringAddr
}

func monitoringTLSConfig(base *tls.Config) *tls.Config {
	var cfg tls.Config
	if base != nil {
		cfg = *base.Clone()
	}
	cfg.NextProtos = withHTTP2Proto(cfg.NextProtos)
	return &cfg
}

func withHTTP2Proto(nextProtos []string) []string {
	out := make([]string, 0, len(nextProtos)+2)
	if !containsString(nextProtos, "h2") {
		out = append(out, "h2")
	}
	if !containsString(nextProtos, "http/1.1") {
		out = append(out, "http/1.1")
	}
	for _, proto := range nextProtos {
		if proto != "h2" && proto != "http/1.1" {
			out = append(out, proto)
		}
	}
	return out
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func loadSnapshotIfConfigured(trie *hatriecache.HatTrie, path string) error {
	if path == "" {
		return nil
	}
	if err := trie.LoadSnapshot(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func startSnapshotSaver(ctx context.Context, trie *hatriecache.HatTrie, path string, interval time.Duration, stderr io.Writer) func() {
	if path == "" || interval <= 0 {
		return func() {}
	}

	ticker := time.NewTicker(interval)
	done := make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := trie.SaveSnapshot(path); err != nil {
					fmt.Fprintf(stderr, "save snapshot: %v\n", err)
				}
			case <-ctx.Done():
				return
			case <-done:
				return
			}
		}
	}()
	return func() {
		close(done)
		<-stopped
	}
}

func snapshotCallback(trie *hatriecache.HatTrie, path string) func() error {
	if path == "" {
		return nil
	}
	return func() error {
		return trie.SaveSnapshot(path)
	}
}
