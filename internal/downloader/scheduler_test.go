package downloader

import (
	"testing"
	"time"
)

func TestPlanChunks(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		size      int64
		chunkSize int64
		wantN     int
		wantLast  [2]int64
	}{
		{"exact", 8 << 20, 4 << 20, 2, [2]int64{4 << 20, (8 << 20) - 1}},
		{"small", 10, 4, 3, [2]int64{8, 9}},
		{"one", 1, 4 << 20, 1, [2]int64{0, 0}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := planChunks(tt.size, tt.chunkSize)
			if len(p.Chunks) != tt.wantN {
				t.Fatalf("chunks=%d want=%d", len(p.Chunks), tt.wantN)
			}
			last := p.Chunks[len(p.Chunks)-1]
			if last.Start != tt.wantLast[0] || last.End != tt.wantLast[1] {
				t.Fatalf("last=[%d,%d] want=[%d,%d]", last.Start, last.End, tt.wantLast[0], tt.wantLast[1])
			}
		})
	}
}

func TestDeriveChunkSize(t *testing.T) {
	t.Parallel()
	tests := []struct {
		bps    float64
		target time.Duration
		wantLo int64
		wantHi int64
	}{
		{0, time.Second, 4 << 20, 4 << 20},
		{800 << 10, time.Second, 1 << 20, 1 << 20},      // clamped to 1 MiB
		{40 << 20, time.Second, 16 << 20, 16 << 20},     // clamped to 16 MiB
		{5 << 20, time.Second, 5 << 20, 6 << 20},        // rounded to MiB boundary (5MiB..6MiB acceptable)
	}
	for _, tt := range tests {
		got := deriveChunkSize(tt.bps, tt.target)
		if got < tt.wantLo || got > tt.wantHi {
			t.Fatalf("bps=%v got=%d want in [%d,%d]", tt.bps, got, tt.wantLo, tt.wantHi)
		}
	}
}

