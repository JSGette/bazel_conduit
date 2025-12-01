// Package graph manages in-memory event graphs for active builds
package graph

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"
)

// Manager manages event graphs for active builds
type Manager struct {
	graphs sync.Map // buildID -> *EventGraph
	config Config
	logger *slog.Logger
	mu     sync.RWMutex

	// Cleanup tracking
	cleanupTimers map[string]*time.Timer
	cleanupMu     sync.Mutex
}

// Config holds configuration for the graph manager
type Config struct {
	MaxConcurrentBuilds int
	BuildTimeout        time.Duration
	CleanupInterval     time.Duration
}

// DefaultConfig returns default configuration
func DefaultConfig() Config {
	return Config{
		MaxConcurrentBuilds: 1000,
		BuildTimeout:        time.Hour,
		CleanupInterval:     5 * time.Minute,
	}
}

// NewManager creates a new event graph manager
func NewManager(config Config, logger *slog.Logger) *Manager {
	if logger == nil {
		logger = slog.Default()
	}

	logger.Info("Initializing event graph manager",
		"max_concurrent_builds", config.MaxConcurrentBuilds,
		"build_timeout", config.BuildTimeout,
	)

	return &Manager{
		config:        config,
		logger:        logger,
		cleanupTimers: make(map[string]*time.Timer),
	}
}

// CreateGraph creates a new event graph for a build
func (m *Manager) CreateGraph(ctx context.Context, buildID string) (*EventGraph, error) {
	if buildID == "" {
		return nil, fmt.Errorf("buildID cannot be empty")
	}

	// Check if graph already exists
	if existing, ok := m.graphs.Load(buildID); ok {
		m.logger.Debug("Event graph already exists",
			"build_id", buildID,
			"trace_id", existing.(*EventGraph).TraceID,
		)
		return existing.(*EventGraph), nil
	}

	// Generate trace ID
	traceID := trace.TraceID(uuid.New())

	graph := &EventGraph{
		BuildID:      buildID,
		TraceID:      traceID.String(),
		Events:       make(map[string]*StoredEvent),
		Spans:        make([]*Span, 0),
		PendingSpans: make(map[string]*Span),
		StartTime:    time.Now(),
	}

	m.graphs.Store(buildID, graph)

	m.logger.Info("Created event graph",
		"build_id", buildID,
		"trace_id", graph.TraceID,
	)

	// Set timeout for cleanup
	m.scheduleTimeout(buildID, m.config.BuildTimeout)

	return graph, nil
}

// GetOrCreateGraph gets an existing graph or creates a new one
func (m *Manager) GetOrCreateGraph(ctx context.Context, buildID, invocationID string) (*EventGraph, error) {
	if buildID == "" {
		return nil, fmt.Errorf("buildID cannot be empty")
	}

	// Try to get existing graph
	if existing, ok := m.graphs.Load(buildID); ok {
		graph := existing.(*EventGraph)

		// Update invocation ID if provided and not set
		if invocationID != "" && graph.InvocationID == "" {
			graph.InvocationID = invocationID
			m.logger.Debug("Updated invocation ID",
				"build_id", buildID,
				"invocation_id", invocationID,
			)
		}

		return graph, nil
	}

	// Create new graph
	graph, err := m.CreateGraph(ctx, buildID)
	if err != nil {
		return nil, err
	}

	graph.InvocationID = invocationID

	return graph, nil
}

// GetGraph retrieves an existing event graph
func (m *Manager) GetGraph(buildID string) *EventGraph {
	if val, ok := m.graphs.Load(buildID); ok {
		return val.(*EventGraph)
	}
	return nil
}

// DeleteGraph removes a graph from memory
func (m *Manager) DeleteGraph(buildID string) {
	if _, ok := m.graphs.LoadAndDelete(buildID); ok {
		m.logger.Info("Deleted event graph",
			"build_id", buildID,
		)
	}

	// Cancel any pending cleanup timer
	m.cancelCleanupTimer(buildID)
}

// ScheduleCleanup schedules cleanup of a graph after a delay
func (m *Manager) ScheduleCleanup(buildID string, delay time.Duration) {
	m.cleanupMu.Lock()
	defer m.cleanupMu.Unlock()

	// Cancel existing timer if any
	if timer, exists := m.cleanupTimers[buildID]; exists {
		timer.Stop()
	}

	timer := time.AfterFunc(delay, func() {
		m.logger.Info("Cleaning up event graph",
			"build_id", buildID,
			"delay", delay,
		)
		m.DeleteGraph(buildID)
	})

	m.cleanupTimers[buildID] = timer

	m.logger.Debug("Scheduled graph cleanup",
		"build_id", buildID,
		"delay", delay,
	)
}

// scheduleTimeout schedules automatic cleanup on timeout
func (m *Manager) scheduleTimeout(buildID string, timeout time.Duration) {
	m.cleanupMu.Lock()
	defer m.cleanupMu.Unlock()

	timer := time.AfterFunc(timeout, func() {
		if graph := m.GetGraph(buildID); graph != nil && !graph.BuildFinished {
			m.logger.Warn("Build timeout, cleaning up graph",
				"build_id", buildID,
				"timeout", timeout,
			)
			m.DeleteGraph(buildID)
		}
	})

	m.cleanupTimers[buildID] = timer
}

// cancelCleanupTimer cancels a pending cleanup timer
func (m *Manager) cancelCleanupTimer(buildID string) {
	m.cleanupMu.Lock()
	defer m.cleanupMu.Unlock()

	if timer, exists := m.cleanupTimers[buildID]; exists {
		timer.Stop()
		delete(m.cleanupTimers, buildID)
	}
}

// ActiveBuilds returns the number of active build graphs
func (m *Manager) ActiveBuilds() int {
	count := 0
	m.graphs.Range(func(key, value interface{}) bool {
		count++
		return true
	})
	return count
}

// Shutdown cleans up all resources
func (m *Manager) Shutdown() {
	m.logger.Info("Shutting down event graph manager")

	// Cancel all cleanup timers
	m.cleanupMu.Lock()
	for buildID, timer := range m.cleanupTimers {
		timer.Stop()
		delete(m.cleanupTimers, buildID)
	}
	m.cleanupMu.Unlock()

	// Clear all graphs
	m.graphs.Range(func(key, value interface{}) bool {
		m.graphs.Delete(key)
		return true
	})

	m.logger.Info("Event graph manager shutdown complete")
}
