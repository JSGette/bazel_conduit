package bes

import (
	"sync"
	"time"

	"go.opentelemetry.io/otel/trace"
)

// SpanRegistry tracks in-progress spans by their identifiers.
// It provides thread-safe access to target and action spans.
type SpanRegistry struct {
	mu sync.RWMutex

	// targetSpans maps target labels to their spans
	targetSpans map[string]trace.Span

	// targetStartTimes maps target labels to their start times
	targetStartTimes map[string]time.Time

	// actionSpans maps action IDs to their spans
	actionSpans map[string]trace.Span

	// actionStartTimes maps action IDs to their start times
	actionStartTimes map[string]time.Time
}

// NewSpanRegistry creates a new SpanRegistry.
func NewSpanRegistry() *SpanRegistry {
	return &SpanRegistry{
		targetSpans:      make(map[string]trace.Span),
		targetStartTimes: make(map[string]time.Time),
		actionSpans:      make(map[string]trace.Span),
		actionStartTimes: make(map[string]time.Time),
	}
}

// AddTarget adds a target span to the registry.
func (r *SpanRegistry) AddTarget(label string, span trace.Span, startTime time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.targetSpans[label] = span
	r.targetStartTimes[label] = startTime
}

// GetTarget retrieves a target span by label.
func (r *SpanRegistry) GetTarget(label string) (trace.Span, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	span, exists := r.targetSpans[label]
	return span, exists
}

// RemoveTarget removes a target span from the registry.
func (r *SpanRegistry) RemoveTarget(label string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.targetSpans, label)
	delete(r.targetStartTimes, label)
}

// GetStartTime retrieves the start time for a target span.
func (r *SpanRegistry) GetStartTime(label string) (time.Time, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	startTime, exists := r.targetStartTimes[label]
	return startTime, exists
}

// AddAction adds an action span to the registry.
func (r *SpanRegistry) AddAction(actionID string, span trace.Span, startTime time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.actionSpans[actionID] = span
	r.actionStartTimes[actionID] = startTime
}

// GetAction retrieves an action span by ID.
func (r *SpanRegistry) GetAction(actionID string) (trace.Span, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	span, exists := r.actionSpans[actionID]
	return span, exists
}

// RemoveAction removes an action span from the registry.
func (r *SpanRegistry) RemoveAction(actionID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.actionSpans, actionID)
	delete(r.actionStartTimes, actionID)
}

// Count returns the total number of target spans in the registry.
func (r *SpanRegistry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return len(r.targetSpans)
}

// GetOrphanedSpans returns labels of target spans that started before the threshold duration.
// These are potentially orphaned spans that never received a completion event.
func (r *SpanRegistry) GetOrphanedSpans(threshold time.Duration) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	cutoff := time.Now().Add(-threshold)
	orphaned := make([]string, 0)

	for label, startTime := range r.targetStartTimes {
		if startTime.Before(cutoff) {
			orphaned = append(orphaned, label)
		}
	}

	return orphaned
}

// GetAllLabels returns all target labels currently in the registry.
func (r *SpanRegistry) GetAllLabels() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	labels := make([]string, 0, len(r.targetSpans))
	for label := range r.targetSpans {
		labels = append(labels, label)
	}

	return labels
}

