package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aravinth/distributed-cache/internal/server"
	"github.com/aravinth/distributed-cache/internal/store"
)

func main() {
	cfg := server.DefaultConfig()

	flag.IntVar(&cfg.Port, "port", cfg.Port, "TCP port to listen on")
	flag.IntVar(&cfg.MaxConnections, "maxclients", cfg.MaxConnections, "Maximum concurrent connections")
	idleTimeout := flag.Int("timeout", 300, "Idle connection timeout in seconds (0 = no timeout)")
	flag.Parse()

	cfg.IdleTimeout = time.Duration(*idleTimeout) * time.Second

	fmt.Print(`
    ____  ______           __
   / __ \/ ____/___ ______/ /_  ___
  / / / / /   / __ '/ ___/ __ \/ _ \
 / /_/ / /___/ /_/ / /__/ / / /  __/
/_____/\____/\__,_/\___/_/ /_/\___/
`)
	log.Printf("starting dcache server")
	log.Printf("  port:       %d", cfg.Port)
	log.Printf("  maxclients: %d", cfg.MaxConnections)
	log.Printf("  timeout:    %ds", *idleTimeout)

	// Initialise the sharded store
	sm := store.NewShardedMap()

	// Create server
	srv := server.New(cfg, sm)

	// Context with signal handling for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigCh
		log.Printf("received signal %s, shutting down...", sig)
		cancel()
	}()

	if err := srv.ListenAndServe(ctx); err != nil {
		log.Fatalf("server error: %v", err)
		os.Exit(1)
	}
}
