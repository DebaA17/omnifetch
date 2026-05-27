package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
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

	var last = time.Now()
	var speedEMA utils.EMA
	var lastBytes float64

	cmd := ytdlp.New().
		NoOverwrites().
		NoCheckCertificates().
		Continue().
		Format("bv*+ba/b").
		Output(outTemplate).
		PrintJSON().
		Progress().
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
				Base:       events.Base{T: now},
				JobID:      job.ID,
				State:      &state,
				BytesDone:   &done,
				BytesTotal:  &total,
				SpeedBpsEMA: ptrF64(speed),
				Workers:    &workers,
			})
		})

	// Quality selection hook: keep default but allow future UI to set it.
	// cmd = cmd.Format("bv*+ba/b")

	res, err := cmd.Run(ctx, job.URL.String())
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return "", omnierr.Canceled(err)
		}
		// Try to surface common typed situations as stable OmniError.
		msg := err.Error()
		switch {
		case strings.Contains(strings.ToLower(msg), "private"):
			return "", omnierr.Wrap(omnierr.CodePrivateContent, "This content appears to be private.", err, omnierr.Retryable(false), omnierr.Sev(omnierr.SeverityWarn))
		case strings.Contains(strings.ToLower(msg), "geo"):
			return "", omnierr.Wrap(omnierr.CodeGeoBlocked, "This content appears geo-blocked.", err, omnierr.Retryable(false), omnierr.Sev(omnierr.SeverityWarn))
		case strings.Contains(strings.ToLower(msg), "429"), strings.Contains(strings.ToLower(msg), "rate"):
			return "", omnierr.Wrap(omnierr.CodeRateLimited, "Rate limited by the remote service.", err, omnierr.Retryable(true), omnierr.Sev(omnierr.SeverityWarn))
		default:
			return "", omnierr.Wrap(omnierr.CodeUnknown, "Media download failed.", err, omnierr.Retryable(true))
		}
	}

	// go-ytdlp doesn't guarantee a single output; pick the first extracted filename if available.
	info, infoErr := res.GetExtractedInfo()
	if infoErr == nil && len(info) > 0 && info[0].Filename != nil && *info[0].Filename != "" {
		out := *info[0].Filename
		emit(events.JobUpdated{Base: events.Base{T: time.Now()}, JobID: job.ID, OutputPath: ptrStr(out)})
		return out, nil
	}

	fallback := filepath.Join(outDir, fmt.Sprintf("%s.%s", job.ID, "media"))
	emit(events.JobLog{Base: events.Base{T: time.Now()}, JobID: job.ID, Level: "info", Message: "yt-dlp finished; output filename unavailable; check media output directory"})
	return fallback, nil
}

func ensureDir(p string) error {
	if err := os.MkdirAll(p, 0o755); err != nil {
		return omnierr.Wrap(omnierr.CodePermissionDenied, "Cannot create output directory.", err, omnierr.Sev(omnierr.SeverityFatal))
	}
	return nil
}

func ptrF64(v float64) *float64 { return &v }

var _ Runner = (*MediaEngine)(nil)
