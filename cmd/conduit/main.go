// Package main is the entry point for the Bazel Conduit service
package main

import (
	"flag"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/JSGette/bazel_conduit/internal/bes"
	"github.com/JSGette/bazel_conduit/internal/graph"
	build "google.golang.org/genproto/googleapis/devtools/build/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

var (
	address = flag.String("address", "localhost:8080", "Address to listen on")
	logJSON = flag.Bool("log-json", false, "Log in JSON format")
)

func main() {
	flag.Parse()

	// Setup logger
	var logger *slog.Logger
	if *logJSON {
		logger = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		}))
	} else {
		logger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		}))
	}

	slog.SetDefault(logger)

	logger.Info("Starting Bazel Conduit",
		"address", *address,
		"version", "0.1.0-dev",
	)

	// Create graph manager
	graphConfig := graph.DefaultConfig()
	graphManager := graph.NewManager(graphConfig, logger)

	// Create BES service
	besService := bes.NewService(graphManager, logger)

	// Create gRPC server
	grpcServer := grpc.NewServer(
		grpc.MaxRecvMsgSize(1024 * 1024 * 10), // 10MB max message size
	)

	// Register the BES service
	build.RegisterPublishBuildEventServer(grpcServer, besService)

	// Enable reflection for debugging with grpcurl
	reflection.Register(grpcServer)

	// Create listener
	listener, err := net.Listen("tcp", *address)
	if err != nil {
		logger.Error("Failed to listen",
			"error", err,
			"address", *address,
		)
		os.Exit(1)
	}

	logger.Info("Server listening",
		"address", listener.Addr().String(),
	)

	// Start server in a goroutine
	serverErrors := make(chan error, 1)
	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			serverErrors <- err
		}
	}()

	// Wait for interrupt signal or server error
	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)

	select {
	case err := <-serverErrors:
		logger.Error("Server error", "error", err)
	case sig := <-shutdown:
		logger.Info("Shutdown signal received", "signal", sig.String())

		// Graceful shutdown
		logger.Info("Shutting down gracefully...")

		// Stop accepting new connections
		grpcServer.GracefulStop()

		// Cleanup graph manager
		graphManager.Shutdown()

		logger.Info("Shutdown complete")
	}
}
