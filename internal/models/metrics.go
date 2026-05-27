package models

import "time"

type LiveMetrics struct {
	ActiveJobs       int
	QueuedJobs       int
	CompletedSuccess int
	CompletedError   int
	BytesPerSecond   float64
	HTTP2Conns       int
	WorkersActive    int
	WorkersMax       int
	Uptime           time.Duration
}

