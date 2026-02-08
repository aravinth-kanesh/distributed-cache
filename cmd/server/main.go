package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/aravinth/distributed-cache/internal/persistence"
	"github.com/aravinth/distributed-cache/internal/server"
	"github.com/aravinth/distributed-cache/internal/store"
)

func main() {
	cfg := server.DefaultConfig()
	pcfg := persistence.DefaultConfig()

	// Server flags
	flag.IntVar(&cfg.Port, "port", cfg.Port, "TCP port to listen on")
	flag.IntVar(&cfg.MaxConnections, "maxclients", cfg.MaxConnections, "Maximum concurrent connections")
	idleTimeout := flag.Int("timeout", 300, "Idle connection timeout in seconds (0 = no timeout)")

	// Persistence flags
	dataDir := flag.String("dir", pcfg.DataDir, "Data directory for persistence files")
	appendOnly := flag.Bool("appendonly", pcfg.AOFEnabled, "Enable append-only file")
	aofFsync := flag.String("aof-fsync", "everysec", "AOF fsync policy: always, everysec, no")
	saveInterval := flag.Int("save-interval", 300, "Snapshot interval in seconds (0 = disabled)")

	flag.Parse()

	cfg.IdleTimeout = time.Duration(*idleTimeout) * time.Second

	// Configure persistence paths
	pcfg.DataDir = *dataDir
	pcfg.AOFEnabled = *appendOnly
	pcfg.AOFFilePath = filepath.Join(*dataDir, "appendonly.aof")
	pcfg.SnapshotDir = *dataDir
	pcfg.SnapshotInterval = time.Duration(*saveInterval) * time.Second
	pcfg.SnapshotEnabled = *saveInterval > 0

	switch *aofFsync {
	case "always":
		pcfg.AOFPolicy = persistence.FsyncAlways
	case "everysec":
		pcfg.AOFPolicy = persistence.FsyncEverySecond
	case "no":
		pcfg.AOFPolicy = persistence.FsyncNone
	default:
		log.Fatalf("invalid aof-fsync policy: %s (must be always, everysec, or no)", *aofFsync)
	}

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
	log.Printf("  datadir:    %s", *dataDir)
	log.Printf("  appendonly: %v", *appendOnly)
	log.Printf("  aof-fsync:  %s", *aofFsync)
	log.Printf("  save-interval: %ds", *saveInterval)

	// Ensure data directory exists
	if err := os.MkdirAll(*dataDir, 0755); err != nil {
		log.Fatalf("failed to create data directory: %v", err)
	}

	// Initialise the sharded store
	sm := store.NewShardedMap()

	// Recovery: load snapshot + replay AOF before accepting connections
	recovery := persistence.NewRecovery(sm, pcfg)
	if _, err := recovery.Run(); err != nil {
		log.Fatalf("recovery failed: %v", err)
	}

	// Initialise persistence components
	var aofWriter *persistence.AOFWriter
	var snapshotEngine *persistence.SnapshotEngine
	var aofRewriter *persistence.AOFRewriter

	if pcfg.AOFEnabled {
		var err error
		aofWriter, err = persistence.NewAOFWriter(pcfg)
		if err != nil {
			log.Fatalf("failed to initialise AOF writer: %v", err)
		}
	}

	if pcfg.SnapshotEnabled {
		snapshotEngine = persistence.NewSnapshotEngine(sm, pcfg, aofWriter)
	}

	if aofWriter != nil {
		aofRewriter = persistence.NewAOFRewriter(sm, pcfg, aofWriter)
	}

	// Create server
	srv := server.New(cfg, sm, aofWriter, snapshotEngine, aofRewriter)

	// Context with signal handling for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start periodic snapshots
	if snapshotEngine != nil {
		snapshotEngine.Start(ctx)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigCh
		log.Printf("received signal %s, shutting down...", sig)
		cancel()
	}()

	if err := srv.ListenAndServe(ctx); err != nil {
		log.Fatalf("server error: %v", err)
	}

	// Shutdown sequencing: flush AOF, take final snapshot
	if aofWriter != nil {
		log.Println("flushing AOF...")
		aofWriter.Close()
	}

	if snapshotEngine != nil {
		log.Println("saving final snapshot...")
		if path, err := snapshotEngine.Save(); err != nil {
			log.Printf("final snapshot failed: %v", err)
		} else {
			log.Printf("final snapshot saved: %s", path)
		}
	}

	log.Println("shutdown complete")
}
