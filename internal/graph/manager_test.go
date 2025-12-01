package graph

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"
)

func TestNewManager(t *testing.T) {
	config := DefaultConfig()
	logger := slog.Default()

	manager := NewManager(config, logger)

	if manager == nil {
		t.Fatal("Expected manager to be created")
	}

	if manager.config.MaxConcurrentBuilds != config.MaxConcurrentBuilds {
		t.Errorf("Expected MaxConcurrentBuilds=%d, got %d",
			config.MaxConcurrentBuilds, manager.config.MaxConcurrentBuilds)
	}
}

func TestCreateGraph(t *testing.T) {
	manager := NewManager(DefaultConfig(), slog.Default())
	ctx := context.Background()

	buildID := "build-123"
	graph, err := manager.CreateGraph(ctx, buildID)

	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if graph == nil {
		t.Fatal("Expected graph to be created")
	}

	if graph.BuildID != buildID {
		t.Errorf("Expected BuildID=%s, got %s", buildID, graph.BuildID)
	}

	if graph.TraceID == "" {
		t.Error("Expected TraceID to be set")
	}

	if graph.StartTime.IsZero() {
		t.Error("Expected StartTime to be set")
	}

	// Verify we can retrieve it
	retrieved := manager.GetGraph(buildID)
	if retrieved != graph {
		t.Error("Expected to retrieve the same graph")
	}
}

func TestCreateGraphEmptyBuildID(t *testing.T) {
	manager := NewManager(DefaultConfig(), slog.Default())
	ctx := context.Background()

	_, err := manager.CreateGraph(ctx, "")

	if err == nil {
		t.Error("Expected error for empty buildID")
	}
}

func TestCreateGraphDuplicate(t *testing.T) {
	manager := NewManager(DefaultConfig(), slog.Default())
	ctx := context.Background()
	buildID := "build-123"

	graph1, err1 := manager.CreateGraph(ctx, buildID)
	if err1 != nil {
		t.Fatalf("Expected no error, got %v", err1)
	}

	graph2, err2 := manager.CreateGraph(ctx, buildID)
	if err2 != nil {
		t.Fatalf("Expected no error, got %v", err2)
	}

	// Should return the same graph
	if graph1 != graph2 {
		t.Error("Expected to get the same graph instance")
	}
}

func TestGetOrCreateGraph(t *testing.T) {
	manager := NewManager(DefaultConfig(), slog.Default())
	ctx := context.Background()

	buildID := "build-123"
	invocationID := "invocation-456"

	// First call creates the graph
	graph1, err := manager.GetOrCreateGraph(ctx, buildID, invocationID)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if graph1.InvocationID != invocationID {
		t.Errorf("Expected InvocationID=%s, got %s", invocationID, graph1.InvocationID)
	}

	// Second call retrieves existing graph
	graph2, err := manager.GetOrCreateGraph(ctx, buildID, "different-invocation")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if graph1 != graph2 {
		t.Error("Expected to get the same graph instance")
	}

	// InvocationID should not change
	if graph2.InvocationID != invocationID {
		t.Errorf("Expected InvocationID to remain %s, got %s", invocationID, graph2.InvocationID)
	}
}

func TestGetOrCreateGraphEmptyBuildID(t *testing.T) {
	manager := NewManager(DefaultConfig(), slog.Default())
	ctx := context.Background()

	_, err := manager.GetOrCreateGraph(ctx, "", "inv-123")

	if err == nil {
		t.Error("Expected error for empty buildID")
	}
}

func TestGetGraph(t *testing.T) {
	manager := NewManager(DefaultConfig(), slog.Default())
	ctx := context.Background()

	buildID := "build-123"

	// Graph doesn't exist yet
	graph := manager.GetGraph(buildID)
	if graph != nil {
		t.Error("Expected nil for non-existent graph")
	}

	// Create graph
	created, _ := manager.CreateGraph(ctx, buildID)

	// Now it should exist
	graph = manager.GetGraph(buildID)
	if graph != created {
		t.Error("Expected to get the created graph")
	}
}

func TestDeleteGraph(t *testing.T) {
	manager := NewManager(DefaultConfig(), slog.Default())
	ctx := context.Background()

	buildID := "build-123"
	manager.CreateGraph(ctx, buildID)

	// Verify it exists
	if manager.GetGraph(buildID) == nil {
		t.Fatal("Expected graph to exist")
	}

	// Delete it
	manager.DeleteGraph(buildID)

	// Verify it's gone
	if manager.GetGraph(buildID) != nil {
		t.Error("Expected graph to be deleted")
	}

	// Delete again should be no-op
	manager.DeleteGraph(buildID) // Should not panic
}

func TestScheduleCleanup(t *testing.T) {
	manager := NewManager(DefaultConfig(), slog.Default())
	ctx := context.Background()

	buildID := "build-123"
	manager.CreateGraph(ctx, buildID)

	// Schedule cleanup with short delay
	delay := 100 * time.Millisecond
	manager.ScheduleCleanup(buildID, delay)

	// Graph should still exist
	if manager.GetGraph(buildID) == nil {
		t.Error("Expected graph to still exist")
	}

	// Wait for cleanup
	time.Sleep(delay + 50*time.Millisecond)

	// Graph should be deleted
	if manager.GetGraph(buildID) != nil {
		t.Error("Expected graph to be deleted after cleanup")
	}
}

func TestScheduleCleanupCancelsPrevious(t *testing.T) {
	manager := NewManager(DefaultConfig(), slog.Default())
	ctx := context.Background()

	buildID := "build-123"
	manager.CreateGraph(ctx, buildID)

	// Schedule cleanup with long delay
	manager.ScheduleCleanup(buildID, time.Hour)

	// Schedule again with short delay (should cancel previous)
	delay := 100 * time.Millisecond
	manager.ScheduleCleanup(buildID, delay)

	// Wait for short cleanup
	time.Sleep(delay + 50*time.Millisecond)

	// Graph should be deleted
	if manager.GetGraph(buildID) != nil {
		t.Error("Expected graph to be deleted")
	}
}

func TestActiveBuilds(t *testing.T) {
	manager := NewManager(DefaultConfig(), slog.Default())
	ctx := context.Background()

	if manager.ActiveBuilds() != 0 {
		t.Error("Expected 0 active builds initially")
	}

	manager.CreateGraph(ctx, "build-1")
	manager.CreateGraph(ctx, "build-2")
	manager.CreateGraph(ctx, "build-3")

	if manager.ActiveBuilds() != 3 {
		t.Errorf("Expected 3 active builds, got %d", manager.ActiveBuilds())
	}

	manager.DeleteGraph("build-2")

	if manager.ActiveBuilds() != 2 {
		t.Errorf("Expected 2 active builds, got %d", manager.ActiveBuilds())
	}
}

func TestShutdown(t *testing.T) {
	manager := NewManager(DefaultConfig(), slog.Default())
	ctx := context.Background()

	// Create some graphs
	manager.CreateGraph(ctx, "build-1")
	manager.CreateGraph(ctx, "build-2")

	// Schedule cleanups
	manager.ScheduleCleanup("build-1", time.Hour)
	manager.ScheduleCleanup("build-2", time.Hour)

	if manager.ActiveBuilds() != 2 {
		t.Fatalf("Expected 2 active builds, got %d", manager.ActiveBuilds())
	}

	// Shutdown
	manager.Shutdown()

	// All graphs should be deleted
	if manager.ActiveBuilds() != 0 {
		t.Errorf("Expected 0 active builds after shutdown, got %d", manager.ActiveBuilds())
	}

	// Cleanup timers should be cancelled
	manager.cleanupMu.Lock()
	timerCount := len(manager.cleanupTimers)
	manager.cleanupMu.Unlock()

	if timerCount != 0 {
		t.Errorf("Expected 0 cleanup timers after shutdown, got %d", timerCount)
	}
}

func TestTimeout(t *testing.T) {
	config := DefaultConfig()
	config.BuildTimeout = 100 * time.Millisecond

	manager := NewManager(config, slog.Default())
	ctx := context.Background()

	buildID := "build-123"
	graph, _ := manager.CreateGraph(ctx, buildID)

	// Don't mark as finished
	graph.BuildFinished = false

	// Wait for timeout
	time.Sleep(config.BuildTimeout + 50*time.Millisecond)

	// Graph should be deleted due to timeout
	if manager.GetGraph(buildID) != nil {
		t.Error("Expected graph to be deleted after timeout")
	}
}

func TestTimeoutDoesNotDeleteFinishedBuild(t *testing.T) {
	config := DefaultConfig()
	config.BuildTimeout = 100 * time.Millisecond

	manager := NewManager(config, slog.Default())
	ctx := context.Background()

	buildID := "build-123"
	graph, _ := manager.CreateGraph(ctx, buildID)

	// Mark as finished immediately
	graph.MarkBuildFinished()

	// Wait for timeout
	time.Sleep(config.BuildTimeout + 50*time.Millisecond)

	// Graph should still exist (finished builds not deleted by timeout)
	if manager.GetGraph(buildID) == nil {
		t.Error("Expected finished build to not be deleted by timeout")
	}
}

func TestConcurrentAccess(t *testing.T) {
	manager := NewManager(DefaultConfig(), slog.Default())
	ctx := context.Background()

	done := make(chan bool)

	// Create graphs concurrently
	for i := 0; i < 10; i++ {
		go func(i int) {
			buildID := fmt.Sprintf("build-%d", i)
			_, err := manager.CreateGraph(ctx, buildID)
			if err != nil {
				t.Errorf("Error creating graph: %v", err)
			}
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}

	if manager.ActiveBuilds() != 10 {
		t.Errorf("Expected 10 active builds, got %d", manager.ActiveBuilds())
	}
}
