package utils

import (
	"math/rand"
	"time"
)

type Backoff struct {
	Base time.Duration
	Max  time.Duration
	Jitter float64 // 0..1
}

func (b Backoff) Duration(attempt int) time.Duration {
	if attempt <= 0 {
		attempt = 1
	}
	d := b.Base << (attempt - 1)
	if d > b.Max {
		d = b.Max
	}
	if b.Jitter <= 0 {
		return d
	}
	j := 1 + (rand.Float64()*2-1)*b.Jitter
	return time.Duration(float64(d) * j)
}

