package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"omnifetch/internal/downloader"
	omnierr "omnifetch/internal/errors"
	"omnifetch/internal/events"
	"omnifetch/internal/models"
	"omnifetch/internal/utils"
)

type FileEngine struct {
	log  *slog.Logger
	dl   *downloader.Downloader
	out  string
	tick time.Duration
	http *http.Client
}

func NewFileEngine(log *slog.Logger, dl *downloader.Downloader, out string, tick time.Duration) *FileEngine {
	if log == nil {
		log = slog.Default()
	}
	return &FileEngine{
		log:  log,
		dl:   dl,
		out:  out,
		tick: tick,
		http: &http.Client{
			Timeout: 0,
			Transport: &http.Transport{
				Proxy:               http.ProxyFromEnvironment,
				ForceAttemptHTTP2:   true,
				MaxIdleConns:        128,
				MaxIdleConnsPerHost: 32,
				IdleConnTimeout:     60 * time.Second,
			},
		},
	}
}

func (*FileEngine) Kind() models.JobKind { return models.JobKindDirectFile }

func (*FileEngine) CanHandle(u *url.URL) bool {
	if u == nil || u.Scheme == "" || u.Host == "" {
		return false
	}
	ext := strings.ToLower(path.Ext(u.Path))
	switch ext {
	case ".zip", ".tar", ".gz", ".bz2", ".xz", ".7z",
		".mp4", ".mkv", ".mov", ".mp3", ".flac", ".wav",
		".pdf", ".epub",
		".png", ".jpg", ".jpeg", ".webp", ".gif",
		".iso", ".dmg", ".exe", ".msi", ".deb", ".rpm":
		return true
	default:
		// Extensionless URLs are common for CDNs (e.g., Google image cache).
		// We'll probe content-type at runtime and derive an extension.
		return u.Scheme == "http" || u.Scheme == "https"
	}
}

func (e *FileEngine) Run(ctx context.Context, job models.Job, emit EmitFunc) (string, error) {
	downloadURL := rewriteGitHubRawURL(job.URL)
	resolvedURL, ct, cd := e.resolveDownloadURL(ctx, downloadURL)
	if resolvedURL == nil {
		resolvedURL = downloadURL
	}

	if isHTMLContentType(ct) {
		msg := "This link looks like a web page, not a direct file. Use the raw file URL instead."
		if strings.Contains(strings.ToLower(job.URL.Host), "github.com") && strings.Contains(job.URL.Path, "/blob/") {
			msg = "This GitHub link points to the repository page, not the raw image file. Use the raw.githubusercontent.com URL instead."
		}
		return "", omnierr.New(omnierr.CodeUnsupportedDomain, msg, omnierr.Details("content-type: "+ct), omnierr.Retryable(false), omnierr.Sev(omnierr.SeverityWarn))
	}

	filename := e.deriveFilename(resolvedURL, cd, ct, string(job.ID))
	outPath := filepath.Join(e.out, filename)

	var lastEmit = time.Now()
	reporter := &jobReporter{
		jobID: job.ID,
		emit:  emit,
		tick:  e.tick,
		last:  lastEmit,
	}

	res, err := e.downloadStreaming(ctx, resolvedURL, outPath, reporter)
	if err != nil {
		return "", err
	}
	emit(events.JobUpdated{
		Base:       events.Base{T: time.Now()},
		JobID:      job.ID,
		OutputPath: ptrStr(res.OutputPath),
	})
	return res.OutputPath, nil
}

func (e *FileEngine) downloadStreaming(ctx context.Context, u *url.URL, outPath string, r *jobReporter) (downloader.Result, error) {
	tmpPath := outPath + ".part"
	if err := os.MkdirAll(filepath.Dir(outPath), 0o755); err != nil {
		return downloader.Result{}, omnierr.Wrap(omnierr.CodePermissionDenied, "Cannot create download directory.", err, omnierr.Sev(omnierr.SeverityFatal))
	}

	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return downloader.Result{}, omnierr.Wrap(omnierr.CodePermissionDenied, "Cannot write to download directory.", err, omnierr.Sev(omnierr.SeverityFatal))
	}
	defer f.Close()

	start := int64(0)
	if fi, err := f.Stat(); err == nil {
		start = fi.Size()
	}

	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	req.Header.Set("User-Agent", "OmniFetch/1.0 (+public-content-downloader)")
	if start > 0 {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", start))
	}

	downloadCtx, cancelDownload := context.WithCancel(ctx)
	defer cancelDownload()

	req = req.WithContext(downloadCtx)
	resp, err := e.http.Do(req)
	if err != nil {
		return downloader.Result{}, omnierr.ClassifyNet("GET", u.String(), err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return downloader.Result{}, omnierr.New(omnierr.CodeHTTP, "Server returned an error.", omnierr.HTTPStatus(resp.StatusCode), omnierr.Details(resp.Status), omnierr.Retryable(resp.StatusCode >= 500 || resp.StatusCode == 429))
	}

	if start > 0 && resp.StatusCode == http.StatusOK {
		if err := f.Truncate(0); err != nil {
			return downloader.Result{}, err
		}
		if _, err := f.Seek(0, 0); err != nil {
			return downloader.Result{}, err
		}
		start = 0
	}

	total := contentLengthFromResponse(resp, start)
	var bytesDone = start
	var lastBytes = bytesDone
	last := time.Now()
	var speedEMA utils.EMA
	var copiedErr error
	doneCh := make(chan error, 1)
	timeoutCh := make(chan struct{}, 1)
	firstByteSeen := make(chan struct{})
	var firstByteOnce sync.Once
	firstByteTimer := time.NewTimer(12 * time.Second)
	defer firstByteTimer.Stop()
	go func() {
		select {
		case <-firstByteSeen:
			if !firstByteTimer.Stop() {
				<-firstByteTimer.C
			}
		case <-firstByteTimer.C:
			timeoutCh <- struct{}{}
			cancelDownload()
		}
	}()

	go func() {
		_, copiedErr = f.Seek(start, 0)
		if copiedErr != nil {
			doneCh <- copiedErr
			return
		}
		buf := make([]byte, 128<<10)
		for {
			n, readErr := resp.Body.Read(buf)
			if n > 0 {
				firstByteOnce.Do(func() { close(firstByteSeen) })
				if _, copiedErr = f.Write(buf[:n]); copiedErr != nil {
					doneCh <- copiedErr
					return
				}
				bytesDone += int64(n)
			}
			if readErr != nil {
				if errors.Is(readErr, io.EOF) {
					doneCh <- nil
					return
				}
				doneCh <- readErr
				return
			}
		}
	}()

	ticker := time.NewTicker(150 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timeoutCh:
			return downloader.Result{}, omnierr.Wrap(omnierr.CodeTimeout, "Timed out waiting for the server to send data.", context.DeadlineExceeded, omnierr.Retryable(true), omnierr.Details(u.String()))
		case <-ctx.Done():
			select {
			case <-timeoutCh:
				return downloader.Result{}, omnierr.Wrap(omnierr.CodeTimeout, "Timed out waiting for the server to send data.", context.DeadlineExceeded, omnierr.Retryable(true), omnierr.Details(u.String()))
			default:
			}
			return downloader.Result{}, omnierr.Canceled(ctx.Err())
		case err := <-doneCh:
			if err != nil {
				select {
				case <-timeoutCh:
					return downloader.Result{}, omnierr.Wrap(omnierr.CodeTimeout, "Timed out waiting for the server to send data.", context.DeadlineExceeded, omnierr.Retryable(true), omnierr.Details(u.String()))
				default:
				}
				return downloader.Result{}, omnierr.ClassifyNet("GET", u.String(), err)
			}
			if err := os.Rename(tmpPath, outPath); err != nil {
				return downloader.Result{}, omnierr.Wrap(omnierr.CodePermissionDenied, "Failed to finalize file.", err, omnierr.Sev(omnierr.SeverityFatal))
			}
			if copiedErr != nil {
				return downloader.Result{}, copiedErr
			}
			return downloader.Result{OutputPath: outPath, Bytes: bytesDone}, nil
		case <-ticker.C:
			now := time.Now()
			dt := now.Sub(last)
			delta := bytesDone - lastBytes
			bps := 0.0
			if dt > 0 {
				bps = float64(delta) / dt.Seconds()
			}
			speed := speedEMA.Add(bps, 2*time.Second, dt)
			last = now
			lastBytes = bytesDone
			r.Progress(downloader.Progress{BytesDone: bytesDone, BytesTotal: total, Bps: speed, Workers: 1})
		}
	}
}

func contentLengthFromResponse(resp *http.Response, start int64) int64 {
	if resp == nil {
		return -1
	}
	if cr := resp.Header.Get("Content-Range"); cr != "" {
		if parts := strings.Split(cr, "/"); len(parts) == 2 {
			if n, err := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64); err == nil {
				return n
			}
		}
	}
	if resp.ContentLength > 0 {
		if resp.StatusCode == http.StatusPartialContent && start > 0 {
			return start + resp.ContentLength
		}
		return resp.ContentLength
	}
	return -1
}

func rewriteGitHubRawURL(u *url.URL) *url.URL {
	if u == nil {
		return nil
	}
	if !strings.EqualFold(u.Host, "github.com") {
		return u
	}
	parts := strings.Split(strings.Trim(u.Path, "/"), "/")
	if len(parts) < 5 || parts[2] != "blob" {
		return u
	}

	raw := *u
	raw.Host = "raw.githubusercontent.com"
	raw.Scheme = "https"

	owner := parts[0]
	repo := parts[1]
	ref := parts[3]
	if ref == "refs" && len(parts) >= 7 && parts[4] == "heads" {
		ref = parts[5]
		parts = append([]string{owner, repo, ref}, parts[6:]...)
	} else {
		parts = append([]string{owner, repo, ref}, parts[4:]...)
	}
	raw.Path = "/" + strings.Join(parts, "/")
	return &raw
}

type remoteProbe struct {
	URL                *url.URL
	ContentType        string
	ContentDisposition string
}

func (e *FileEngine) resolveDownloadURL(ctx context.Context, u *url.URL) (*url.URL, string, string) {
	probe := func(method string, addRange bool) remoteProbe {
		probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		req, _ := http.NewRequestWithContext(probeCtx, method, u.String(), nil)
		req.Header.Set("User-Agent", "OmniFetch/1.0 (+public-content-downloader)")
		if addRange {
			req.Header.Set("Range", "bytes=0-0")
		}
		resp, err := e.http.Do(req)
		if err != nil || resp == nil {
			return remoteProbe{}
		}
		defer resp.Body.Close()
		if method == http.MethodHead && (resp.StatusCode == http.StatusMethodNotAllowed || resp.StatusCode == http.StatusForbidden) {
			return remoteProbe{}
		}
		if resp.StatusCode >= 400 {
			return remoteProbe{URL: resp.Request.URL, ContentType: strings.ToLower(resp.Header.Get("Content-Type")), ContentDisposition: resp.Header.Get("Content-Disposition")}
		}
		return remoteProbe{
			URL:                resp.Request.URL,
			ContentType:        strings.ToLower(resp.Header.Get("Content-Type")),
			ContentDisposition: resp.Header.Get("Content-Disposition"),
		}
	}

	if p := probe(http.MethodHead, false); p.URL != nil {
		return p.URL, p.ContentType, p.ContentDisposition
	}
	if p := probe(http.MethodGet, true); p.URL != nil {
		return p.URL, p.ContentType, p.ContentDisposition
	}
	return u, "", ""
}

func isHTMLContentType(ct string) bool {
	ct = strings.ToLower(ct)
	return strings.Contains(ct, "text/html") || strings.Contains(ct, "application/xhtml")
}

func (e *FileEngine) deriveFilename(u *url.URL, contentDisposition string, contentType string, fallbackBase string) string {
	// 1) Try path basename (if it has an extension).
	if cd := contentDisposition; cd != "" {
		if _, params, perr := mime.ParseMediaType(cd); perr == nil {
			if fn := params["filename"]; fn != "" {
				return sanitizeFilename(fn)
			}
		}
	}

	if dot := extFromContentType(contentType); dot != "" {
		base := filepath.Base(u.Path)
		if base != "" && base != "/" && base != "." {
			if strings.TrimSpace(filepath.Ext(base)) != "" {
				return sanitizeFilename(base)
			}
		}
		return sanitizeFilename(fallbackBase + dot)
	}

	base := filepath.Base(u.Path)
	ext := strings.ToLower(filepath.Ext(base))
	if base != "" && base != "/" && base != "." && ext != "" {
		return sanitizeFilename(base)
	}

	return sanitizeFilename(fallbackBase + ".bin")
}

func extFromContentType(ct string) string {
	if ct == "" {
		return ""
	}
	// Strip charset.
	if i := strings.Index(ct, ";"); i >= 0 {
		ct = strings.TrimSpace(ct[:i])
	}
	switch ct {
	case "image/jpeg":
		return ".jpg"
	case "image/png":
		return ".png"
	case "image/webp":
		return ".webp"
	case "image/gif":
		return ".gif"
	case "image/avif":
		return ".avif"
	case "image/svg+xml":
		return ".svg"
	case "application/pdf":
		return ".pdf"
	case "application/zip":
		return ".zip"
	}
	if strings.HasPrefix(ct, "text/plain") {
		return ".txt"
	}
	return ""
}

func sanitizeFilename(s string) string {
	s = strings.TrimSpace(s)
	s = strings.ReplaceAll(s, string(filepath.Separator), "_")
	if s == "" {
		return "download.bin"
	}
	return s
}

type jobReporter struct {
	jobID models.JobID
	emit  EmitFunc
	tick  time.Duration
	last  time.Time
}

func (r *jobReporter) Progress(p downloader.Progress) {
	now := time.Now()
	if r.tick > 0 && now.Sub(r.last) < r.tick {
		return
	}
	r.last = now
	state := models.StateDownloading
	done := p.BytesDone
	total := p.BytesTotal
	speed := p.Bps
	workers := p.Workers
	r.emit(events.JobUpdated{
		Base:        events.Base{T: now},
		JobID:       r.jobID,
		State:       &state,
		BytesDone:   &done,
		BytesTotal:  &total,
		SpeedBpsEMA: &speed,
		Workers:     &workers,
	})
}

func (r *jobReporter) Logf(level string, format string, args ...any) {
	r.emit(events.JobLog{
		Base:    events.Base{T: time.Now()},
		JobID:   r.jobID,
		Level:   level,
		Message: fmt.Sprintf(format, args...),
	})
}

func ptrStr(s string) *string { return &s }

// compile-time assert
var _ Runner = (*FileEngine)(nil)
