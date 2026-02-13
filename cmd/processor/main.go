package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"db-stream/internal/config"
	"db-stream/internal/processor"
	"db-stream/pkg/logger"

	"go.uber.org/zap"
)

func main() {
	// Initialize logger
	log := logger.New("processor-service")

	// Load configuration
	cfg, err := config.Load("processor-service")
	if err != nil {
		log.Fatal("Failed to load configuration", zap.Error(err))
	}

	log.Info("Starting Processor service",
		zap.String("version", "1.0.0"),
		zap.String("service", cfg.Service.Name),
	)

	// Initialize event processor
	proc, err := processor.NewEventProcessor(cfg, log)
	if err != nil {
		log.Fatal("Failed to create event processor", zap.Error(err))
	}

	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start processor in goroutine
	go func() {
		if err := proc.Start(ctx); err != nil {
			log.Error("Event processor failed", zap.Error(err))
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

	log.Info("Shutting down Processor service...")
	if err := proc.Shutdown(shutdownCtx); err != nil {
		log.Error("Error during shutdown", zap.Error(err))
	} else {
		log.Info("Processor service shutdown completed successfully")
	}
}
