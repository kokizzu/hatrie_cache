package main

import (
	"context"
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
	monitoringServer bool
	monitoringAddr   string
	monitoringWebDir string
	snapshotPath     string
	snapshotInterval time.Duration
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
		WebDir: cfg.monitoringWebDir,
	}).Handler()
	server := &http.Server{
		Addr:    cfg.monitoringAddr,
		Handler: handler,
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- server.ListenAndServe()
	}()
	fmt.Fprintf(stdout, "monitoring server listening on %s\n", cfg.monitoringAddr)

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
	flags.StringVar(&cfg.monitoringWebDir, "monitoring-web-dir", cfg.monitoringWebDir, "directory containing built web monitoring assets")
	flags.StringVar(&cfg.snapshotPath, "snapshot-path", "", "optional JSON snapshot path to load on startup and save on shutdown")
	flags.DurationVar(&cfg.snapshotInterval, "snapshot-interval", 0, "optional periodic snapshot interval")
	if err := flags.Parse(args); err != nil {
		return config{}, err
	}
	return cfg, nil
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
