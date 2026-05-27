package utils

import (
	"math"
	"time"
)

// EMA implements an exponential moving average that is stable across variable tick intervals.
type EMA struct {
	value float64
	set   bool
}

func (e *EMA) Add(sample float64, halfLife time.Duration, dt time.Duration) float64 {
	if dt <= 0 {
		return e.value
	}
	if !e.set {
		e.set = true
		e.value = sample
		return e.value
	}
	// alpha = 1 - 0.5^(dt/halfLife)
	alpha := 1 - powHalf(dt, halfLife)
	e.value = e.value + alpha*(sample-e.value)
	return e.value
}

func (e *EMA) Value() float64 { return e.value }

func powHalf(dt, halfLife time.Duration) float64 {
	if halfLife <= 0 {
		return 0
	}
	// 0.5^(dt/halfLife)
	return math.Pow(0.5, float64(dt)/float64(halfLife))
}
