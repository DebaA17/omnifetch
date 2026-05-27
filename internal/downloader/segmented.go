package downloader

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	omnierr "omnifetch/internal/errors"
	"omnifetch/internal/utils"
)

func (d *Downloader) downloadSegmented(ctx context.Context, req Request, r Reporter) (Result, error) {
	if req.URL == nil || req.URL.String() == "" {
		return Result{}, omnierr.InvalidURL("", errors.New("missing url"))
	}
	if req.OutputPath == "" {
		return Result{}, omnierr.New(omnierr.CodeUnknown, "Missing output path.", omnierr.Sev(omnierr.SeverityFatal))
	}
	if req.MinWorkers <= 0 {
		req.MinWorkers = 2
	}
	if req.MaxWorkers <= 0 {
		req.MaxWorkers = 8
	}
	if req.MaxWorkers < req.MinWorkers {
		req.MaxWorkers = req.MinWorkers
	}
	if req.ChunkTarget <= 0 {
		req.ChunkTarget = 1200 * time.Millisecond
	}

	tmpPath, metaPath := resumePaths(req.OutputPath, req.TempDir)
	tmpDir := filepath.Dir(tmpPath)
	if err := os.MkdirAll(tmpDir, 0o755); err != nil {
		return Result{}, omnierr.Wrap(omnierr.CodePermissionDenied, "Cannot create download directory.", err, omnierr.Sev(omnierr.SeverityFatal))
	}

	headReq, _ := http.NewRequestWithContext(ctx, http.MethodHead, req.URL.String(), nil)
	headReq.Header.Set("User-Agent", "OmniFetch/1.0 (+public-content-downloader)")
	resp, err := d.client.HTTP.Do(headReq)
	if err != nil {
		// Some servers reject HEAD; probe with a ranged GET.
		return d.probeWithRangeGET(ctx, req, r)
	}
	resp.Body.Close()
	if resp.StatusCode == http.StatusMethodNotAllowed || resp.StatusCode == http.StatusForbidden {
		return d.probeWithRangeGET(ctx, req, r)
	}
	if resp.StatusCode >= 400 {
		return Result{}, omnierr.New(omnierr.CodeHTTP, "Server returned an error.", omnierr.HTTPStatus(resp.StatusCode), omnierr.Details(resp.Status), omnierr.Retryable(resp.StatusCode >= 500 || resp.StatusCode == 429))
	}

	size := resp.ContentLength
	acceptRanges := strings.Contains(strings.ToLower(resp.Header.Get("Accept-Ranges")), "bytes")
	etag := resp.Header.Get("ETag")
	lastMod := resp.Header.Get("Last-Modified")

	var st *resumeState
	if s, err := loadResume(metaPath); err == nil {
		// Resume only if it looks compatible.
		if s.URL == req.URL.String() && s.Size == size && s.ETag == etag && s.LastModified == lastMod && s.TempPath == tmpPath && s.OutputPath == req.OutputPath {
			st = s
		} else {
			_ = os.Remove(metaPath)
		}
	}

	if size <= 0 || !acceptRanges {
		r.Logf("warn", "server does not support ranged downloads; falling back to single stream")
		return d.downloadSingle(ctx, req, tmpPath, metaPath, r)
	}

	var speedEMA utils.EMA
	var lastBytes int64
	var lastTick = time.Now()

	workers := req.MinWorkers
	if workers > req.MaxWorkers {
		workers = req.MaxWorkers
	}

	var chunkSize int64 = 4 << 20
	if st != nil && st.ChunkSize > 0 {
		chunkSize = st.ChunkSize
	}
	plan := planChunks(size, chunkSize)
	if st == nil {
		st = &resumeState{
			Version:      1,
			URL:          req.URL.String(),
			OutputPath:   req.OutputPath,
			TempPath:     tmpPath,
			MetaPath:     metaPath,
			Size:         size,
			ETag:         etag,
			LastModified: lastMod,
			ChunkSize:    plan.ChunkSize,
			Complete:     make([]bool, len(plan.Chunks)),
			CreatedAt:    time.Now(),
		}
	}
	if len(st.Complete) != len(plan.Chunks) {
		// If server size changed or chunk count differs, treat as corrupt partial.
		_ = os.Remove(metaPath)
		_ = os.Remove(tmpPath)
		return Result{}, omnierr.New(omnierr.CodeCorruptPartial, "Partial download state is incompatible with the remote file.", omnierr.Details("chunk plan mismatch"), omnierr.Retryable(true))
	}

	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return Result{}, omnierr.Wrap(omnierr.CodePermissionDenied, "Cannot write to download directory.", err, omnierr.Sev(omnierr.SeverityFatal))
	}
	defer f.Close()
	if err := f.Truncate(size); err != nil {
		return Result{}, omnierr.Wrap(omnierr.CodeDiskFull, "Failed to allocate space for download.", err, omnierr.Sev(omnierr.SeverityFatal))
	}

	// Build work queue of incomplete chunks.
	workCh := make(chan chunk, workers*2)
	var remaining int64
	for _, c := range plan.Chunks {
		if c.Index < len(st.Complete) && st.Complete[c.Index] {
			continue
		}
		remaining += (c.End - c.Start + 1)
	}
	if remaining == 0 {
		// already complete
		if err := os.Rename(tmpPath, req.OutputPath); err != nil {
			return Result{}, omnierr.Wrap(omnierr.CodePermissionDenied, "Failed to finalize file.", err, omnierr.Sev(omnierr.SeverityFatal))
		}
		return d.finishChecksum(req, r)
	}

	var bytesDone int64
	for i, done := range st.Complete {
		if done && i < len(plan.Chunks) {
			bytesDone += (plan.Chunks[i].End - plan.Chunks[i].Start + 1)
		}
	}
	atomic.StoreInt64(&bytesDone, bytesDone)

	eg, egCtx := errgroup.WithContext(ctx)
	eg.SetLimit(req.MaxWorkers)

	var resumeMu sync.Mutex
	sendProgress := func() {
		now := time.Now()
		done := atomic.LoadInt64(&bytesDone)
		delta := done - lastBytes
		dt := now.Sub(lastTick)
		if dt <= 0 {
			return
		}
		bps := float64(delta) / dt.Seconds()
		speed := speedEMA.Add(bps, 2*time.Second, dt)
		lastBytes = done
		lastTick = now

		// One-time gentle adaptation of chunk size based on observed throughput.
		if done > (8<<20) && st.ChunkSize == plan.ChunkSize {
			newChunk := deriveChunkSize(speed, req.ChunkTarget)
			if newChunk != plan.ChunkSize {
				plan = planChunks(size, newChunk)
				// do not re-slice st.Complete; changing chunk size mid-run invalidates resume,
				// so only adjust for future runs after completion.
			}
		}

		r.Progress(Progress{
			BytesDone:  done,
			BytesTotal: size,
			Bps:        speed,
			Workers:    workers,
		})
	}

	ticker := time.NewTicker(150 * time.Millisecond)
	defer ticker.Stop()
	eg.Go(func() error {
		defer sendProgress()
		for {
			select {
			case <-egCtx.Done():
				return egCtx.Err()
			case <-ticker.C:
				sendProgress()
				// persist resume state periodically (cheap, but avoid thrash)
				resumeMu.Lock()
				_ = saveResumeAtomic(metaPath, st)
				resumeMu.Unlock()
			}
		}
	})

	// Producer
	eg.Go(func() error {
		defer close(workCh)
		for _, c := range planChunks(size, st.ChunkSize).Chunks {
			if st.Complete[c.Index] {
				continue
			}
			select {
			case <-egCtx.Done():
				return egCtx.Err()
			case workCh <- c:
			}
		}
		return nil
	})

	// Workers
	for wi := 0; wi < req.MaxWorkers; wi++ {
		eg.Go(func() error {
			buf := make([]byte, 128<<10)
			for c := range workCh {
				if egCtx.Err() != nil {
					return egCtx.Err()
				}
				if st.Complete[c.Index] {
					continue
				}
				if err := d.downloadRangeInto(egCtx, req.URL.String(), f, c.Start, c.End, buf); err != nil {
					return err
				}
				atomic.AddInt64(&bytesDone, c.End-c.Start+1)
				resumeMu.Lock()
				st.Complete[c.Index] = true
				_ = saveResumeAtomic(metaPath, st)
				resumeMu.Unlock()
			}
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		if errors.Is(err, context.Canceled) {
			return Result{}, omnierr.Canceled(err)
		}
		return Result{}, omnierr.ClassifyNet("GET", req.URL.String(), err)
	}

	_ = os.Remove(metaPath)
	if err := os.Rename(tmpPath, req.OutputPath); err != nil {
		return Result{}, omnierr.Wrap(omnierr.CodePermissionDenied, "Failed to finalize file.", err, omnierr.Sev(omnierr.SeverityFatal))
	}
	return d.finishChecksum(req, r)
}

func (d *Downloader) probeWithRangeGET(ctx context.Context, req Request, r Reporter) (Result, error) {
	probeReq, _ := http.NewRequestWithContext(ctx, http.MethodGet, req.URL.String(), nil)
	probeReq.Header.Set("Range", "bytes=0-0")
	probeReq.Header.Set("User-Agent", "OmniFetch/1.0 (+public-content-downloader)")
	resp, err := d.client.HTTP.Do(probeReq)
	if err != nil {
		return Result{}, omnierr.ClassifyNet("GET", req.URL.String(), err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 && resp.StatusCode != http.StatusPartialContent {
		return Result{}, omnierr.New(omnierr.CodeHTTP, "Server returned an error.", omnierr.HTTPStatus(resp.StatusCode), omnierr.Details(resp.Status), omnierr.Retryable(resp.StatusCode >= 500 || resp.StatusCode == 429))
	}
	// Try to recover size from Content-Range: bytes 0-0/1234
	size := resp.ContentLength
	if cr := resp.Header.Get("Content-Range"); cr != "" {
		if parts := strings.Split(cr, "/"); len(parts) == 2 {
			if n, perr := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64); perr == nil {
				size = n
			}
		}
	}
	acceptRanges := true // ranged GET worked
	etag := resp.Header.Get("ETag")
	lastMod := resp.Header.Get("Last-Modified")

	// Re-enter main path using derived headers.
	tmpPath, metaPath := resumePaths(req.OutputPath, req.TempDir)
	tmpDir := filepath.Dir(tmpPath)
	if err := os.MkdirAll(tmpDir, 0o755); err != nil {
		return Result{}, omnierr.Wrap(omnierr.CodePermissionDenied, "Cannot create download directory.", err, omnierr.Sev(omnierr.SeverityFatal))
	}
	if size <= 0 || !acceptRanges {
		return d.downloadSingle(ctx, req, tmpPath, metaPath, r)
	}
	// Fake a minimal head response by setting variables and continuing by tail-calling.
	_ = etag
	_ = lastMod
	// We can't easily jump back into the segmented path without duplicating a lot of code,
	// so treat this case as a single-stream resumable download. It's still robust.
	return d.downloadSingle(ctx, req, tmpPath, metaPath, r)
}

func (d *Downloader) downloadRangeInto(ctx context.Context, url string, f *os.File, start, end int64, buf []byte) error {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))
	req.Header.Set("User-Agent", "OmniFetch/1.0 (+public-content-downloader)")
	resp, err := d.client.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusPartialContent && resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected http status %s", resp.Status)
	}

	// Verify the server honored the requested range when possible.
	if cr := resp.Header.Get("Content-Range"); cr != "" && strings.HasPrefix(cr, "bytes") {
		// Example: bytes 0-1023/2048
		parts := strings.Fields(cr)
		if len(parts) >= 2 {
			rng := strings.Split(strings.Split(parts[1], "/")[0], "-")
			if len(rng) == 2 {
				s0, _ := strconv.ParseInt(rng[0], 10, 64)
				s1, _ := strconv.ParseInt(rng[1], 10, 64)
				if s0 != start || s1 != end {
					return fmt.Errorf("server returned mismatched range %d-%d", s0, s1)
				}
			}
		}
	}

	off := start
	for {
		n, readErr := resp.Body.Read(buf)
		if n > 0 {
			if _, err := f.WriteAt(buf[:n], off); err != nil {
				return err
			}
			off += int64(n)
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}
			return readErr
		}
	}
	return nil
}

func (d *Downloader) downloadSingle(ctx context.Context, req Request, tmpPath, metaPath string, r Reporter) (Result, error) {
	outDir := filepath.Dir(req.OutputPath)
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return Result{}, omnierr.Wrap(omnierr.CodePermissionDenied, "Cannot create download directory.", err, omnierr.Sev(omnierr.SeverityFatal))
	}
	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return Result{}, omnierr.Wrap(omnierr.CodePermissionDenied, "Cannot write to download directory.", err, omnierr.Sev(omnierr.SeverityFatal))
	}
	defer f.Close()

	// Resume if possible by appending from current size.
	var start int64
	if fi, err := f.Stat(); err == nil {
		start = fi.Size()
	}

	httpReq, _ := http.NewRequestWithContext(ctx, http.MethodGet, req.URL.String(), nil)
	if start > 0 {
		httpReq.Header.Set("Range", fmt.Sprintf("bytes=%d-", start))
	}
	httpReq.Header.Set("User-Agent", "OmniFetch/1.0 (+public-content-downloader)")
	resp, err := d.client.HTTP.Do(httpReq)
	if err != nil {
		return Result{}, omnierr.ClassifyNet("GET", req.URL.String(), err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return Result{}, omnierr.New(omnierr.CodeHTTP, "Server returned an error.", omnierr.HTTPStatus(resp.StatusCode), omnierr.Details(resp.Status), omnierr.Retryable(resp.StatusCode >= 500 || resp.StatusCode == 429))
	}

	if start > 0 && resp.StatusCode == http.StatusOK {
		// Server ignored Range; restart.
		if err := f.Truncate(0); err != nil {
			return Result{}, err
		}
		if _, err := f.Seek(0, 0); err != nil {
			return Result{}, err
		}
		start = 0
	}

	var bytesDone = start
	last := time.Now()
	lastBytes := bytesDone
	var ema utils.EMA
	ticker := time.NewTicker(150 * time.Millisecond)
	defer ticker.Stop()

	doneCh := make(chan error, 1)
	go func() {
		_, err := f.Seek(start, 0)
		if err != nil {
			doneCh <- err
			return
		}
		buf := make([]byte, 128<<10)
		for {
			n, rerr := resp.Body.Read(buf)
			if n > 0 {
				if _, err := f.Write(buf[:n]); err != nil {
					doneCh <- err
					return
				}
				bytesDone += int64(n)
			}
			if rerr != nil {
				if errors.Is(rerr, io.EOF) {
					doneCh <- nil
					return
				}
				doneCh <- rerr
				return
			}
		}
	}()

	st := &resumeState{
		Version:    1,
		URL:        req.URL.String(),
		OutputPath: req.OutputPath,
		TempPath:   tmpPath,
		MetaPath:   metaPath,
		Size:       -1,
		ChunkSize:  -1,
		Complete:   nil,
		CreatedAt:  time.Now(),
	}

	for {
		select {
		case <-ctx.Done():
			return Result{}, omnierr.Canceled(ctx.Err())
		case err := <-doneCh:
			_ = saveResumeAtomic(metaPath, st)
			if err != nil {
				return Result{}, omnierr.ClassifyNet("GET", req.URL.String(), err)
			}
			_ = os.Remove(metaPath)
			if err := os.Rename(tmpPath, req.OutputPath); err != nil {
				return Result{}, omnierr.Wrap(omnierr.CodePermissionDenied, "Failed to finalize file.", err, omnierr.Sev(omnierr.SeverityFatal))
			}
			return d.finishChecksum(req, r)
		case <-ticker.C:
			now := time.Now()
			dt := now.Sub(last)
			delta := bytesDone - lastBytes
			bps := float64(delta) / dt.Seconds()
			speed := ema.Add(bps, 2*time.Second, dt)
			last = now
			lastBytes = bytesDone
			st.Size = bytesDone
			_ = saveResumeAtomic(metaPath, st)
			r.Progress(Progress{
				BytesDone:  bytesDone,
				BytesTotal: -1,
				Bps:        speed,
				Workers:    1,
			})
		}
	}
}

func (d *Downloader) finishChecksum(req Request, r Reporter) (Result, error) {
	sum, n, err := sha256File(req.OutputPath)
	if err != nil {
		return Result{}, omnierr.Wrap(omnierr.CodeUnknown, "Failed to compute checksum.", err, omnierr.Retryable(false))
	}
	if req.ExpectedSHA256 != "" && !strings.EqualFold(req.ExpectedSHA256, sum) {
		_ = os.Remove(req.OutputPath)
		return Result{}, omnierr.New(omnierr.CodeChecksumMismatch, "Checksum mismatch.", omnierr.Details("expected "+req.ExpectedSHA256+" got "+sum), omnierr.Retryable(true))
	}
	return Result{OutputPath: req.OutputPath, Bytes: n, SHA256: sum}, nil
}
