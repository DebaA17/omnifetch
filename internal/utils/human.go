package utils

import (
	"fmt"
	"math"
	"time"
)

func HumanBytes(n int64) string {
	if n < 0 {
		return "-" + HumanBytes(-n)
	}
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for n >= div*unit && exp < 6 {
		div *= unit
		exp++
	}
	value := float64(n) / float64(div)
	suffix := []string{"KiB", "MiB", "GiB", "TiB", "PiB", "EiB"}[exp]
	if value >= 10 {
		return fmt.Sprintf("%.0f %s", value, suffix)
	}
	return fmt.Sprintf("%.1f %s", value, suffix)
}

func HumanRate(bps float64) string {
	if bps <= 0 {
		return "0 B/s"
	}
	return HumanBytes(int64(math.Round(bps))) + "/s"
}

func HumanDur(d time.Duration) string {
	if d < 0 {
		d = -d
	}
	secs := int(d.Seconds())
	if secs < 60 {
		return fmt.Sprintf("%ds", secs)
	}
	mins := secs / 60
	secs %= 60
	if mins < 60 {
		return fmt.Sprintf("%dm%02ds", mins, secs)
	}
	hrs := mins / 60
	mins %= 60
	return fmt.Sprintf("%dh%02dm", hrs, mins)
}

