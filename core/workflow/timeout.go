package workflow

import (
	"sync"
	"time"
)

// CancelFunc cancels a scheduled timeout.
type CancelFunc func()

// StepTimeoutManager schedules and cancels per-workflow step timeouts.
type StepTimeoutManager struct {
	mu     sync.Mutex
	timers map[string]*time.Timer
}

// NewStepTimeoutManager creates a StepTimeoutManager.
func NewStepTimeoutManager() *StepTimeoutManager {
	return &StepTimeoutManager{
		timers: make(map[string]*time.Timer),
	}
}

// Schedule registers a timeout for the given workflowID. If a previous timer
// exists it is cancelled first. The onTimeout callback is invoked after duration.
// Returns a CancelFunc that can stop the timer before it fires.
func (m *StepTimeoutManager) Schedule(workflowID string, duration time.Duration, onTimeout func()) CancelFunc {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Cancel any existing timer for this workflow.
	if t, ok := m.timers[workflowID]; ok {
		t.Stop()
	}

	timer := time.AfterFunc(duration, func() {
		m.mu.Lock()
		delete(m.timers, workflowID)
		m.mu.Unlock()
		onTimeout()
	})
	m.timers[workflowID] = timer

	return func() {
		m.Cancel(workflowID)
	}
}

// Cancel stops the scheduled timeout for workflowID. Safe to call multiple times.
func (m *StepTimeoutManager) Cancel(workflowID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if t, ok := m.timers[workflowID]; ok {
		t.Stop()
		delete(m.timers, workflowID)
	}
}
