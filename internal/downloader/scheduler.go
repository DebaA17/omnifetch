package downloader

import "time"

type chunkPlan struct {
	ChunkSize int64
	Chunks    []chunk
}

type chunk struct {
	Index int
	Start int64
	End   int64 // inclusive
}

func planChunks(size int64, chunkSize int64) chunkPlan {
	if chunkSize <= 0 {
		chunkSize = 4 << 20
	}
	if size <= 0 {
		return chunkPlan{ChunkSize: chunkSize}
	}
	var chunks []chunk
	var idx int
	for off := int64(0); off < size; off += chunkSize {
		end := off + chunkSize - 1
		if end >= size {
			end = size - 1
		}
		chunks = append(chunks, chunk{Index: idx, Start: off, End: end})
		idx++
	}
	return chunkPlan{ChunkSize: chunkSize, Chunks: chunks}
}

func deriveChunkSize(bps float64, target time.Duration) int64 {
	if bps <= 0 {
		return 4 << 20
	}
	if target <= 0 {
		target = 1200 * time.Millisecond
	}
	ideal := int64(bps * target.Seconds())
	// clamp 1MiB .. 16MiB
	if ideal < 1<<20 {
		ideal = 1 << 20
	}
	if ideal > 16<<20 {
		ideal = 16 << 20
	}
	// round to MiB
	ideal = (ideal + (1<<20 - 1)) &^ (1<<20 - 1)
	return ideal
}

