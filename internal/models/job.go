package models

import (
	"net/url"
	"time"
)

type JobID string

type JobKind uint8

const (
	JobKindUnknown JobKind = iota
	JobKindDirectFile
	JobKindArticle
	JobKindMedia
)

type JobState uint8

const (
	StateQueued JobState = iota + 1
	StateDownloading
	StateRetrying
	StateSuccess
	StateError
	StateCanceled
)

type Job struct {
	ID        JobID
	CreatedAt time.Time
	UpdatedAt time.Time

	Kind JobKind
	URL  *url.URL

	// OutputPath is the final destination file/dir (depending on engine).
	OutputPath string

	State JobState

	// ErrorSummary is a short, user-friendly error message.
	ErrorSummary string

	// ErrorDetails is technical info for a details view (kept short).
	ErrorDetails string

	Retryable bool

	// Progress fields are updated via events (UI thread owns display state).
	BytesDone   int64
	BytesTotal  int64
	SpeedBpsEMA float64
	Workers     int

	Attempts int
}

func (s JobState) String() string {
	switch s {
	case StateQueued:
		return "QUEUED"
	case StateDownloading:
		return "DOWNLOADING"
	case StateRetrying:
		return "RETRYING"
	case StateSuccess:
		return "SUCCESS"
	case StateError:
		return "ERROR"
	case StateCanceled:
		return "CANCELED"
	default:
		return "UNKNOWN"
	}
}

func (k JobKind) String() string {
	switch k {
	case JobKindDirectFile:
		return "FILE"
	case JobKindArticle:
		return "ARTICLE"
	case JobKindMedia:
		return "MEDIA"
	default:
		return "UNKNOWN"
	}
}

