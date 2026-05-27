package engine

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	ytdlp "github.com/lrstanley/go-ytdlp"

	omnierr "omnifetch/internal/errors"
	"omnifetch/internal/events"
	"omnifetch/internal/models"
	"omnifetch/internal/utils"
)

type MediaEngine struct {
	log  *slog.Logger
	out  string
	tick time.Duration
}

func NewMediaEngine(log *slog.Logger, out string, tick time.Duration) *MediaEngine {
	if log == nil {
		log = slog.Default()
	}
	return &MediaEngine{log: log, out: out, tick: tick}
}

func (*MediaEngine) Kind() models.JobKind { return models.JobKindMedia }

func (*MediaEngine) CanHandle(u *url.URL) bool {
	if u == nil || u.Host == "" {
		return false
	}
	h := strings.ToLower(u.Host)
	switch {
	case strings.Contains(h, "youtube.com"), strings.Contains(h, "youtu.be"),
		strings.Contains(h, "instagram.com"),
		strings.Contains(h, "tiktok.com"),
		strings.Contains(h, "facebook.com"):
		return true
	default:
		return false
	}
}

func (e *MediaEngine) Run(ctx context.Context, job models.Job, emit EmitFunc) (string, error) {
	// Ensure yt-dlp is present; this keeps OmniFetch keyless and zero-config.
	_, err := ytdlp.Install(ctx, &ytdlp.InstallOptions{})
	if err != nil {
		return "", omnierr.Wrap(omnierr.CodeUnknown, "Failed to install or resolve yt-dlp.", err, omnierr.Retryable(true))
	}

	// Also ensure ffmpeg is available for merging formats when required.
	_, _ = ytdlp.InstallFFmpeg(ctx, &ytdlp.InstallFFmpegOptions{})
	_, _ = ytdlp.InstallFFprobe(ctx, &ytdlp.InstallFFmpegOptions{})

	outDir := filepath.Join(e.out, "media")
	if err := ensureDir(outDir); err != nil {
		return "", err
	}

	// Use a stable template; actual filename is determined by yt-dlp.
	outTemplate := filepath.Join(outDir, "%(title).120B [%(id)s].%(ext)s")
	beforeFiles := snapshotMediaFiles(outDir)

	var last = time.Now()
	var speedEMA utils.EMA
	var lastBytes float64
	var lastStderrLine string

	host := strings.ToLower(job.URL.Host)

	cmd := ytdlp.New().
		NoOverwrites().
		NoCheckCertificates().
		Retries("10").
		RetrySleep("http:linear=1:3").
		Continue().
		Format("bv*+ba/b").
		Output(outTemplate).
		PrintJSON().
		Progress().
		StderrFunc(func(line string) {
			ln := strings.TrimSpace(line)
			if ln == "" {
				return
			}
			lastStderrLine = ln
		}).
		ProgressFunc(120*time.Millisecond, func(update ytdlp.ProgressUpdate) {
			now := time.Now()
			dt := now.Sub(last)
			last = now

			total := int64(update.TotalBytes)
			done := int64(update.DownloadedBytes)

			// Derive instantaneous bps from downloaded bytes (yt-dlp provides speed too, but keep stable).
			delta := float64(done) - lastBytes
			lastBytes = float64(done)
			bps := 0.0
			if dt > 0 {
				bps = delta / dt.Seconds()
			}
			speed := speedEMA.Add(bps, 2*time.Second, dt)

			workers := 1
			state := models.StateDownloading
			if update.Status == ytdlp.ProgressStatusError {
				state = models.StateRetrying
			}

			emit(events.JobUpdated{
				Base:        events.Base{T: now},
				JobID:       job.ID,
				State:       &state,
				BytesDone:   &done,
				BytesTotal:  &total,
				SpeedBpsEMA: ptrF64(speed),
				Workers:     &workers,
			})
		})

	if strings.Contains(host, "instagram.com") {
		cmd = cmd.
			AddHeaders("Referer:https://www.instagram.com/").
			AddHeaders("User-Agent:Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36")
	}

	if v := strings.TrimSpace(os.Getenv("OMNIFETCH_COOKIES_FROM_BROWSER")); v != "" {
		cmd = cmd.CookiesFromBrowser(v)
	}
	if v := strings.TrimSpace(os.Getenv("OMNIFETCH_COOKIES_FILE")); v != "" {
		cmd = cmd.Cookies(v)
	}

	// Quality selection hook: keep default but allow future UI to set it.
	// cmd = cmd.Format("bv*+ba/b")

	res, err := cmd.Run(ctx, job.URL.String())
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return "", omnierr.Canceled(err)
		}
		// Try to surface common typed situations as stable OmniError.
		msg := strings.ToLower(strings.TrimSpace(err.Error() + "\n" + lastStderrLine))
		switch {
		case strings.Contains(msg, "isn't available to everyone"),
			strings.Contains(msg, "can't be seen by certain audiences"),
			strings.Contains(msg, "login"),
			strings.Contains(msg, "cookies"),
			strings.Contains(msg, "not a bot"),
			strings.Contains(msg, "checkpoint required"):
			return "", omnierr.Wrap(omnierr.CodePrivateContent, "This media host blocked anonymous access. Retry with browser cookies.", err, omnierr.Details(strings.TrimSpace(lastStderrLine)), omnierr.Retryable(true), omnierr.Sev(omnierr.SeverityWarn))
		case strings.Contains(strings.ToLower(msg), "private"):
			return "", omnierr.Wrap(omnierr.CodePrivateContent, "This content appears to be private.", err, omnierr.Details(strings.TrimSpace(lastStderrLine)), omnierr.Retryable(false), omnierr.Sev(omnierr.SeverityWarn))
		case strings.Contains(strings.ToLower(msg), "geo"):
			return "", omnierr.Wrap(omnierr.CodeGeoBlocked, "This content appears geo-blocked.", err, omnierr.Details(strings.TrimSpace(lastStderrLine)), omnierr.Retryable(false), omnierr.Sev(omnierr.SeverityWarn))
		case strings.Contains(strings.ToLower(msg), "429"), strings.Contains(strings.ToLower(msg), "rate"):
			return "", omnierr.Wrap(omnierr.CodeRateLimited, "Rate limited by the remote service.", err, omnierr.Details(strings.TrimSpace(lastStderrLine)), omnierr.Retryable(true), omnierr.Sev(omnierr.SeverityWarn))
		default:
			return "", omnierr.Wrap(omnierr.CodeUnknown, "Media download failed.", err, omnierr.Details(strings.TrimSpace(lastStderrLine)), omnierr.Retryable(true))
		}
	}

	info, _ := res.GetExtractedInfo()
	downloaded := collectDownloadedFiles(info, outDir, beforeFiles)

	if len(downloaded) == 0 {
		msg := strings.ToLower(res.Stderr + "\n" + lastStderrLine)
		detail := strings.TrimSpace(lastStderrLine)
		if detail == "" {
			detail = strings.TrimSpace(res.Stderr)
		}
		if strings.Contains(msg, "downloading 0 items") {
			return "", omnierr.Wrap(
				omnierr.CodePrivateContent,
				"No downloadable media items were found for this post.",
				errors.New("yt-dlp reported 0 items"),
				omnierr.Details(detail),
				omnierr.Retryable(true),
				omnierr.Sev(omnierr.SeverityWarn),
			)
		}
		return "", omnierr.Wrap(
			omnierr.CodeUnknown,
			"Media extractor finished but produced no output files.",
			errors.New("no media files created"),
			omnierr.Details(detail),
			omnierr.Retryable(true),
		)
	}

	if len(downloaded) == 1 {
		out := downloaded[0]
		emit(events.JobUpdated{Base: events.Base{T: time.Now()}, JobID: job.ID, OutputPath: ptrStr(out)})
		return out, nil
	}

	emit(events.JobLog{
		Base:    events.Base{T: time.Now()},
		JobID:   job.ID,
		Level:   "info",
		Message: fmt.Sprintf("Downloaded %d media files", len(downloaded)),
	})
	emit(events.JobUpdated{Base: events.Base{T: time.Now()}, JobID: job.ID, OutputPath: ptrStr(outDir)})
	return outDir, nil
}

func ensureDir(p string) error {
	if err := os.MkdirAll(p, 0o755); err != nil {
		return omnierr.Wrap(omnierr.CodePermissionDenied, "Cannot create output directory.", err, omnierr.Sev(omnierr.SeverityFatal))
	}
	return nil
}

func ptrF64(v float64) *float64 { return &v }

type mediaFileSig struct {
	Size    int64
	ModTime time.Time
}

func snapshotMediaFiles(dir string) map[string]mediaFileSig {
	snap := make(map[string]mediaFileSig)
	_ = filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d == nil || d.IsDir() {
			return nil
		}
		info, statErr := d.Info()
		if statErr != nil {
			return nil
		}
		snap[filepath.Clean(path)] = mediaFileSig{Size: info.Size(), ModTime: info.ModTime()}
		return nil
	})
	return snap
}

func collectDownloadedFiles(info []*ytdlp.ExtractedInfo, outDir string, before map[string]mediaFileSig) []string {
	seen := make(map[string]struct{})
	addIfExists := func(path string) {
		if strings.TrimSpace(path) == "" {
			return
		}
		p := path
		if !filepath.IsAbs(p) {
			p = filepath.Join(outDir, p)
		}
		p = filepath.Clean(p)
		st, err := os.Stat(p)
		if err != nil || st.IsDir() {
			return
		}
		seen[p] = struct{}{}
	}

	var walkInfo func(items []*ytdlp.ExtractedInfo)
	walkInfo = func(items []*ytdlp.ExtractedInfo) {
		for _, item := range items {
			if item == nil {
				continue
			}
			if item.Filename != nil {
				addIfExists(*item.Filename)
			}
			if item.AltFilename != nil {
				addIfExists(*item.AltFilename)
			}
			if len(item.Entries) > 0 {
				walkInfo(item.Entries)
			}
		}
	}
	walkInfo(info)

	_ = filepath.WalkDir(outDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d == nil || d.IsDir() {
			return nil
		}
		info, statErr := d.Info()
		if statErr != nil {
			return nil
		}
		p := filepath.Clean(path)
		prev, ok := before[p]
		if !ok || prev.Size != info.Size() || !prev.ModTime.Equal(info.ModTime()) {
			seen[p] = struct{}{}
		}
		return nil
	})

	files := make([]string, 0, len(seen))
	for p := range seen {
		files = append(files, p)
	}
	sort.Strings(files)
	return files
}

var _ Runner = (*MediaEngine)(nil)
