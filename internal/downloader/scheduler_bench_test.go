package downloader

import "testing"

func BenchmarkPlanChunks(b *testing.B) {
	size := int64(4 << 30)      // 4 GiB
	chunk := int64(4 << 20)     // 4 MiB
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = planChunks(size, chunk)
	}
}

