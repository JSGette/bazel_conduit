package bes

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// Helper to create a test tracer
func newTestTracer() (trace.Tracer, *tracetest.SpanRecorder) {
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	return tp.Tracer("test"), recorder
}

// TestNewSpanRegistry tests creating a new SpanRegistry
func TestNewSpanRegistry(t *testing.T) {
	registry := NewSpanRegistry()

	if registry == nil {
		t.Fatal("NewSpanRegistry returned nil")
	}
	if registry.Count() != 0 {
		t.Errorf("expected 0 spans, got %d", registry.Count())
	}
}

// TestSpanRegistryAddTarget tests adding a target span
func TestSpanRegistryAddTarget(t *testing.T) {
	registry := NewSpanRegistry()
	tracer, _ := newTestTracer()

	ctx := context.Background()
	_, span := tracer.Start(ctx, "//deps/foo:bar")

	startTime := time.Now()
	registry.AddTarget("//deps/foo:bar", span, startTime)

	if registry.Count() != 1 {
		t.Errorf("expected 1 span, got %d", registry.Count())
	}

	retrieved, exists := registry.GetTarget("//deps/foo:bar")
	if !exists {
		t.Fatal("span not found in registry")
	}
	if retrieved != span {
		t.Error("retrieved span doesn't match stored span")
	}
}

// TestSpanRegistryAddMultipleTargets tests adding multiple target spans
func TestSpanRegistryAddMultipleTargets(t *testing.T) {
	registry := NewSpanRegistry()
	tracer, _ := newTestTracer()

	ctx := context.Background()
	labels := []string{"//deps/a:lib", "//deps/b:lib", "//deps/c:lib"}

	for _, label := range labels {
		_, span := tracer.Start(ctx, label)
		registry.AddTarget(label, span, time.Now())
	}

	if registry.Count() != 3 {
		t.Errorf("expected 3 spans, got %d", registry.Count())
	}

	for _, label := range labels {
		if _, exists := registry.GetTarget(label); !exists {
			t.Errorf("span %s not found", label)
		}
	}
}

// TestSpanRegistryRemoveTarget tests removing a target span
func TestSpanRegistryRemoveTarget(t *testing.T) {
	registry := NewSpanRegistry()
	tracer, _ := newTestTracer()

	ctx := context.Background()
	_, span := tracer.Start(ctx, "//deps/foo:bar")
	registry.AddTarget("//deps/foo:bar", span, time.Now())

	// Remove the span
	registry.RemoveTarget("//deps/foo:bar")

	if registry.Count() != 0 {
		t.Errorf("expected 0 spans after removal, got %d", registry.Count())
	}

	_, exists := registry.GetTarget("//deps/foo:bar")
	if exists {
		t.Error("span should not exist after removal")
	}
}

// TestSpanRegistryRemoveNonexistent tests removing a span that doesn't exist
func TestSpanRegistryRemoveNonexistent(t *testing.T) {
	registry := NewSpanRegistry()

	// This should not panic
	registry.RemoveTarget("//nonexistent:target")

	if registry.Count() != 0 {
		t.Errorf("expected 0 spans, got %d", registry.Count())
	}
}

// TestSpanRegistryGetNonexistent tests getting a span that doesn't exist
func TestSpanRegistryGetNonexistent(t *testing.T) {
	registry := NewSpanRegistry()

	span, exists := registry.GetTarget("//nonexistent:target")
	if exists {
		t.Error("expected exists to be false")
	}
	if span != nil {
		t.Error("expected span to be nil")
	}
}

// TestSpanRegistryGetStartTime tests getting the start time of a span
func TestSpanRegistryGetStartTime(t *testing.T) {
	registry := NewSpanRegistry()
	tracer, _ := newTestTracer()

	ctx := context.Background()
	_, span := tracer.Start(ctx, "//deps/foo:bar")

	startTime := time.Date(2025, 12, 3, 9, 43, 50, 0, time.UTC)
	registry.AddTarget("//deps/foo:bar", span, startTime)

	retrievedTime, exists := registry.GetStartTime("//deps/foo:bar")
	if !exists {
		t.Fatal("start time not found")
	}
	if !retrievedTime.Equal(startTime) {
		t.Errorf("start time mismatch: got %v, want %v", retrievedTime, startTime)
	}
}

// TestSpanRegistryGetStartTimeNonexistent tests getting start time for non-existent span
func TestSpanRegistryGetStartTimeNonexistent(t *testing.T) {
	registry := NewSpanRegistry()

	_, exists := registry.GetStartTime("//nonexistent:target")
	if exists {
		t.Error("expected exists to be false")
	}
}

// TestSpanRegistryGetOrphanedSpans tests finding spans older than a threshold
func TestSpanRegistryGetOrphanedSpans(t *testing.T) {
	registry := NewSpanRegistry()
	tracer, _ := newTestTracer()

	ctx := context.Background()

	// Add an old span
	oldTime := time.Now().Add(-time.Hour)
	_, oldSpan := tracer.Start(ctx, "//deps/old:target")
	registry.AddTarget("//deps/old:target", oldSpan, oldTime)

	// Add a recent span
	recentTime := time.Now()
	_, recentSpan := tracer.Start(ctx, "//deps/recent:target")
	registry.AddTarget("//deps/recent:target", recentSpan, recentTime)

	// Get orphaned spans (older than 30 minutes)
	threshold := 30 * time.Minute
	orphaned := registry.GetOrphanedSpans(threshold)

	if len(orphaned) != 1 {
		t.Errorf("expected 1 orphaned span, got %d", len(orphaned))
	}
	if len(orphaned) > 0 && orphaned[0] != "//deps/old:target" {
		t.Errorf("expected orphaned span to be //deps/old:target, got %s", orphaned[0])
	}
}

// TestSpanRegistryGetOrphanedSpansEmpty tests when no spans are orphaned
func TestSpanRegistryGetOrphanedSpansEmpty(t *testing.T) {
	registry := NewSpanRegistry()
	tracer, _ := newTestTracer()

	ctx := context.Background()

	// Add recent spans
	_, span := tracer.Start(ctx, "//deps/foo:bar")
	registry.AddTarget("//deps/foo:bar", span, time.Now())

	orphaned := registry.GetOrphanedSpans(time.Hour)

	if len(orphaned) != 0 {
		t.Errorf("expected 0 orphaned spans, got %d", len(orphaned))
	}
}

// TestSpanRegistryGetAllLabels tests getting all registered labels
func TestSpanRegistryGetAllLabels(t *testing.T) {
	registry := NewSpanRegistry()
	tracer, _ := newTestTracer()

	ctx := context.Background()
	labels := []string{"//deps/a:lib", "//deps/b:lib", "//deps/c:lib"}

	for _, label := range labels {
		_, span := tracer.Start(ctx, label)
		registry.AddTarget(label, span, time.Now())
	}

	allLabels := registry.GetAllLabels()

	if len(allLabels) != 3 {
		t.Errorf("expected 3 labels, got %d", len(allLabels))
	}

	// Verify all original labels are present
	labelMap := make(map[string]bool)
	for _, l := range allLabels {
		labelMap[l] = true
	}
	for _, label := range labels {
		if !labelMap[label] {
			t.Errorf("label %s not found in GetAllLabels()", label)
		}
	}
}

// TestSpanRegistryAddAction tests adding an action span
func TestSpanRegistryAddAction(t *testing.T) {
	registry := NewSpanRegistry()
	tracer, _ := newTestTracer()

	ctx := context.Background()

	// Add parent target span first
	_, targetSpan := tracer.Start(ctx, "//deps/foo:bar")
	registry.AddTarget("//deps/foo:bar", targetSpan, time.Now())

	// Add action span
	_, actionSpan := tracer.Start(ctx, "CppCompile")
	actionSpan.SetAttributes(attribute.String("action.mnemonic", "CppCompile"))
	actionID := "action-123"
	registry.AddAction(actionID, actionSpan, time.Now())

	retrieved, exists := registry.GetAction(actionID)
	if !exists {
		t.Fatal("action span not found")
	}
	if retrieved != actionSpan {
		t.Error("retrieved action span doesn't match stored span")
	}
}

// TestSpanRegistryRemoveAction tests removing an action span
func TestSpanRegistryRemoveAction(t *testing.T) {
	registry := NewSpanRegistry()
	tracer, _ := newTestTracer()

	ctx := context.Background()
	_, actionSpan := tracer.Start(ctx, "CppCompile")
	actionID := "action-123"
	registry.AddAction(actionID, actionSpan, time.Now())

	registry.RemoveAction(actionID)

	_, exists := registry.GetAction(actionID)
	if exists {
		t.Error("action span should not exist after removal")
	}
}

// TestSpanRegistryConcurrentAccess tests thread safety
func TestSpanRegistryConcurrentAccess(t *testing.T) {
	registry := NewSpanRegistry()
	tracer, _ := newTestTracer()

	ctx := context.Background()
	done := make(chan bool)

	// Concurrent writes
	for i := 0; i < 100; i++ {
		go func(n int) {
			label := "//deps/target:" + string(rune('a'+n%26))
			_, span := tracer.Start(ctx, label)
			registry.AddTarget(label, span, time.Now())
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 100; i++ {
		<-done
	}

	// Count should be <= 26 (might have duplicates)
	if registry.Count() == 0 {
		t.Error("expected some spans in registry")
	}
}

