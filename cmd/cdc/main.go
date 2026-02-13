package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"db-stream/internal/cdc"
	"db-stream/internal/config"
	"db-stream/pkg/logger"

	"go.uber.org/zap"
)

func main() {
	// Initialize logger
	log := logger.New("cdc-service")

	// Load configuration
	cfg, err := config.Load("cdc-service")
	if err != nil {
		log.Fatal("Failed to load configuration", zap.Error(err))
	}

	log.Info("Starting CDC service",
		zap.String("version", "1.0.0"),
		zap.String("service", cfg.Service.Name),
	)

	// Initialize CDC processor
	processor, err := cdc.NewProcessor(cfg, log)
	if err != nil {
		log.Fatal("Failed to create CDC processor", zap.Error(err))
	}

	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start CDC processor in goroutine
	go func() {
		if err := processor.Start(ctx); err != nil {
			log.Error("CDC processor failed", zap.Error(err))
			cancel()
		}
	}()

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigChan:
		log.Info("Received shutdown signal", zap.String("signal", sig.String()))
	case <-ctx.Done():
		log.Info("Context cancelled, shutting down")
	}

	// Graceful shutdown with timeout
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	log.Info("Shutting down CDC service...")
	if err := processor.Shutdown(shutdownCtx); err != nil {
		log.Error("Error during shutdown", zap.Error(err))
	} else {
		log.Info("CDC service shutdown completed successfully")
	}
}
