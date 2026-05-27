package queue

import (
	"slices"
	"sync"
	"time"

	"omnifetch/internal/models"
)

// Queue is the authoritative job state store. It is safe for concurrent use.
// The UI should not read it directly; instead, it consumes immutable events.
type Queue struct {
	mu   sync.RWMutex
	jobs []models.Job
	byID map[models.JobID]int
}

func New() *Queue {
	return &Queue{
		byID: make(map[models.JobID]int),
	}
}

func (q *Queue) Add(job models.Job) {
	q.mu.Lock()
	defer q.mu.Unlock()
	job.UpdatedAt = time.Now()
	q.byID[job.ID] = len(q.jobs)
	q.jobs = append(q.jobs, job)
}

func (q *Queue) Get(id models.JobID) (models.Job, bool) {
	q.mu.RLock()
	defer q.mu.RUnlock()
	idx, ok := q.byID[id]
	if !ok || idx < 0 || idx >= len(q.jobs) {
		return models.Job{}, false
	}
	return q.jobs[idx], true
}

func (q *Queue) Snapshot() []models.Job {
	q.mu.RLock()
	defer q.mu.RUnlock()
	out := make([]models.Job, len(q.jobs))
	copy(out, q.jobs)
	return out
}

type Patch struct {
	State        *models.JobState
	BytesDone    *int64
	BytesTotal   *int64
	SpeedBpsEMA  *float64
	Workers      *int
	ErrorSummary *string
	ErrorDetails *string
	Retryable    *bool
	Attempts     *int
	OutputPath   *string
}

func (q *Queue) Patch(id models.JobID, p Patch) (models.Job, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	idx, ok := q.byID[id]
	if !ok || idx < 0 || idx >= len(q.jobs) {
		return models.Job{}, false
	}
	j := q.jobs[idx]
	j.UpdatedAt = time.Now()
	if p.State != nil {
		j.State = *p.State
	}
	if p.BytesDone != nil {
		j.BytesDone = *p.BytesDone
	}
	if p.BytesTotal != nil {
		j.BytesTotal = *p.BytesTotal
	}
	if p.SpeedBpsEMA != nil {
		j.SpeedBpsEMA = *p.SpeedBpsEMA
	}
	if p.Workers != nil {
		j.Workers = *p.Workers
	}
	if p.ErrorSummary != nil {
		j.ErrorSummary = *p.ErrorSummary
	}
	if p.ErrorDetails != nil {
		j.ErrorDetails = *p.ErrorDetails
	}
	if p.Retryable != nil {
		j.Retryable = *p.Retryable
	}
	if p.Attempts != nil {
		j.Attempts = *p.Attempts
	}
	if p.OutputPath != nil {
		j.OutputPath = *p.OutputPath
	}
	q.jobs[idx] = j
	return j, true
}

func (q *Queue) RemoveWhere(fn func(models.Job) bool) []models.JobID {
	q.mu.Lock()
	defer q.mu.Unlock()
	var removed []models.JobID
	dst := q.jobs[:0]
	for _, j := range q.jobs {
		if fn(j) {
			removed = append(removed, j.ID)
			delete(q.byID, j.ID)
			continue
		}
		dst = append(dst, j)
	}
	q.jobs = dst
	// rebuild index map (order preserved)
	q.byID = make(map[models.JobID]int, len(q.jobs))
	for i := range q.jobs {
		q.byID[q.jobs[i].ID] = i
	}
	return removed
}

func (q *Queue) IDsWhere(fn func(models.Job) bool) []models.JobID {
	q.mu.RLock()
	defer q.mu.RUnlock()
	out := make([]models.JobID, 0, 8)
	for _, j := range q.jobs {
		if fn(j) {
			out = append(out, j.ID)
		}
	}
	return out
}

func (q *Queue) SortByUpdatedDesc() {
	q.mu.Lock()
	defer q.mu.Unlock()
	slices.SortFunc(q.jobs, func(a, b models.Job) int {
		if a.UpdatedAt.After(b.UpdatedAt) {
			return -1
		}
		if a.UpdatedAt.Before(b.UpdatedAt) {
			return 1
		}
		return 0
	})
	q.byID = make(map[models.JobID]int, len(q.jobs))
	for i := range q.jobs {
		q.byID[q.jobs[i].ID] = i
	}
}

