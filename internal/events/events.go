package events

import (
	"time"

	"omnifetch/internal/models"
)

type Kind uint8

const (
	KindJobAdded Kind = iota + 1
	KindJobUpdated
	KindJobRemoved
	KindJobLog
	KindMetrics
)

type Event interface {
	Kind() Kind
	At() time.Time
}

type Base struct {
	T time.Time
}

func (b Base) At() time.Time { return b.T }

type JobAdded struct {
	Base
	Job models.Job
}

func (JobAdded) Kind() Kind { return KindJobAdded }

type JobUpdated struct {
	Base
	JobID models.JobID

	// Patch-style: only fields that change are set (zero values are meaningful,
	// so use pointers).
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

func (JobUpdated) Kind() Kind { return KindJobUpdated }

type JobRemoved struct {
	Base
	JobID models.JobID
}

func (JobRemoved) Kind() Kind { return KindJobRemoved }

type JobLog struct {
	Base
	JobID  models.JobID
	Level  string
	Message string
}

func (JobLog) Kind() Kind { return KindJobLog }

type Metrics struct {
	Base
	Live models.LiveMetrics
}

func (Metrics) Kind() Kind { return KindMetrics }

