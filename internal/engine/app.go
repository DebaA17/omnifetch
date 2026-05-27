package engine

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"omnifetch/internal/downloader"
	omnierr "omnifetch/internal/errors"
	"omnifetch/internal/events"
	"omnifetch/internal/models"
	"omnifetch/internal/queue"
	"omnifetch/internal/utils"
)

type Config struct {
	Log *slog.Logger

	DefaultOutputDir string

	// ProgressTickEvery is the global max frequency the app emits progress patches.
	ProgressTickEvery time.Duration
}

type App struct {
	log *slog.Logger

	q *queue.Queue

	eventsCh chan events.Event

	downloadClient *downloader.Client
	fileDL         *downloader.Downloader

	engines []Runner

	mu          sync.Mutex
	jobCancels  map[models.JobID]context.CancelFunc
	jobRunning  map[models.JobID]struct{}
	startedAt   time.Time
	outputDir   string
	progressHz  time.Duration
	closed      bool
}

type Runner interface {
	Kind() models.JobKind
	CanHandle(u *url.URL) bool

	// Run performs the job and returns the final output path.
	Run(ctx context.Context, job models.Job, emit EmitFunc) (string, error)
}

type EmitFunc func(events.Event)

func NewApp(cfg Config) (*App, error) {
	log := cfg.Log
	if log == nil {
		log = slog.Default()
	}
	out := cfg.DefaultOutputDir
	if out == "" {
		out = "downloads"
	}
	if cfg.ProgressTickEvery <= 0 {
		cfg.ProgressTickEvery = 120 * time.Millisecond
	}
	if err := os.MkdirAll(out, 0o755); err != nil {
		return nil, omnierr.Wrap(omnierr.CodePermissionDenied, "Cannot create output directory.", err, omnierr.Sev(omnierr.SeverityFatal))
	}

	client := downloader.NewClient()
	app := &App{
		log:           log,
		q:             queue.New(),
		eventsCh:      make(chan events.Event, 2048),
		downloadClient: client,
		fileDL:        downloader.New(client),
		jobCancels:    make(map[models.JobID]context.CancelFunc),
		jobRunning:    make(map[models.JobID]struct{}),
		startedAt:     time.Now(),
		outputDir:     out,
		progressHz:    cfg.ProgressTickEvery,
	}

	app.engines = []Runner{
		NewMediaEngine(log, out, cfg.ProgressTickEvery),
		NewFileEngine(log, app.fileDL, out, cfg.ProgressTickEvery),
		NewArticleEngine(log, out),
	}
	return app, nil
}

func (a *App) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return nil
	}
	a.closed = true
	for _, cancel := range a.jobCancels {
		cancel()
	}
	close(a.eventsCh)
	return nil
}

func (a *App) Events() <-chan events.Event { return a.eventsCh }

// Snapshot returns a point-in-time copy of all known jobs.
func (a *App) Snapshot() []models.Job { return a.q.Snapshot() }

// Done returns true when there are no queued/downloading/retrying jobs.
func (a *App) Done() bool {
	snap := a.q.Snapshot()
	for _, j := range snap {
		switch j.State {
		case models.StateQueued, models.StateDownloading, models.StateRetrying:
			return false
		}
	}
	return true
}

func (a *App) emit(ev events.Event) {
	select {
	case a.eventsCh <- ev:
	default:
		// UI is behind; drop non-critical events to maintain responsiveness.
	}
}

func (a *App) AddURL(raw string) (models.JobID, error) {
	raw = strings.TrimSpace(raw)
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" || u.Host == "" {
		// Common UX improvement: accept bare domains by assuming https.
		if !strings.Contains(raw, "://") {
			if u2, err2 := url.Parse("https://" + raw); err2 == nil && u2.Host != "" {
				u = u2
				err = nil
			}
		}
		if err != nil || u == nil || u.Scheme == "" || u.Host == "" {
			return "", omnierr.InvalidURL(raw, err)
		}
	}

	r := a.route(u)
	if r == nil {
		return "", omnierr.UnsupportedDomain(u.Host)
	}

	id := models.JobID(utils.RandHex(8))
	job := models.Job{
		ID:        id,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
		Kind:      r.Kind(),
		URL:       u,
		State:     models.StateQueued,
	}

	// Set a default output path; engines may overwrite later.
	job.OutputPath = filepath.Join(a.outputDir, string(id))

	a.q.Add(job)
	a.emit(events.JobAdded{Base: events.Base{T: time.Now()}, Job: job})
	a.emit(a.metricsEvent())
	return id, nil
}

func (a *App) route(u *url.URL) Runner {
	for _, e := range a.engines {
		if e.CanHandle(u) {
			return e
		}
	}
	return nil
}

// StartQueued begins downloading all QUEUED jobs, limited by maxParallel.
func (a *App) StartQueued(ctx context.Context, maxParallel int) {
	if maxParallel <= 0 {
		maxParallel = 3
	}
	ids := a.q.IDsWhere(func(j models.Job) bool { return j.State == models.StateQueued })
	if len(ids) == 0 {
		return
	}

	eg, egCtx := errgroup.WithContext(ctx)
	eg.SetLimit(maxParallel)
	for _, id := range ids {
		id := id
		eg.Go(func() error {
			a.startOne(egCtx, id)
			return nil
		})
	}
	go func() { _ = eg.Wait() }()
}

func (a *App) startOne(ctx context.Context, id models.JobID) {
	job, ok := a.q.Get(id)
	if !ok {
		return
	}

	a.mu.Lock()
	if _, running := a.jobRunning[id]; running {
		a.mu.Unlock()
		return
	}
	jobCtx, cancel := context.WithCancel(ctx)
	a.jobCancels[id] = cancel
	a.jobRunning[id] = struct{}{}
	a.mu.Unlock()

	defer func() {
		a.mu.Lock()
		delete(a.jobRunning, id)
		delete(a.jobCancels, id)
		a.mu.Unlock()
		a.emit(a.metricsEvent())
	}()

	runner := a.selectRunner(jobCtx, job.URL)
	if runner == nil {
		msg := "No engine supports this URL."
		a.patchJob(id, queue.Patch{State: ptrState(models.StateError), ErrorSummary: &msg, Retryable: ptrBool(false)})
		return
	}

	attempts := job.Attempts + 1
	a.patchJob(id, queue.Patch{Attempts: &attempts, State: ptrState(models.StateDownloading)})

	out, err := runner.Run(jobCtx, job, a.emit)
	if err != nil {
		oe, ok := omnierr.As(err)
		summary := "Failed."
		details := omnierr.FormatTechnical(err)
		retryable := true
		if ok {
			summary = oe.UserMsg
			if oe.Details != "" {
				details = oe.Details
			}
			retryable = oe.Retryable
		}
		state := models.StateError
		if errors.Is(err, context.Canceled) || omnierr.IsCode(err, omnierr.CodeCanceled) {
			state = models.StateCanceled
			retryable = false
			summary = "Canceled."
		}
		a.patchJob(id, queue.Patch{
			State:        ptrState(state),
			ErrorSummary: &summary,
			ErrorDetails: &details,
			Retryable:    &retryable,
		})
		return
	}

	a.patchJob(id, queue.Patch{
		State:      ptrState(models.StateSuccess),
		OutputPath: &out,
	})
}

func (a *App) CancelActive() {
	a.mu.Lock()
	defer a.mu.Unlock()
	for id, cancel := range a.jobCancels {
		_ = id
		cancel()
	}
}

func (a *App) selectRunner(ctx context.Context, u *url.URL) Runner {
	// Prefer explicit media host routing.
	for _, e := range a.engines {
		if e.Kind() == models.JobKindMedia && e.CanHandle(u) {
			return e
		}
	}
	// Prefer explicit file extensions next.
	for _, e := range a.engines {
		if e.Kind() == models.JobKindDirectFile && e.CanHandle(u) {
			return e
		}
	}
	// Probe content-type quickly to decide Article vs File for extensionless URLs.
	kind := a.probeKind(ctx, u)
	for _, e := range a.engines {
		if e.Kind() == kind && e.CanHandle(u) {
			return e
		}
	}
	// Fallback: article (handles most http pages), then file.
	for _, e := range a.engines {
		if e.Kind() == models.JobKindArticle && e.CanHandle(u) {
			return e
		}
	}
	for _, e := range a.engines {
		if e.Kind() == models.JobKindDirectFile && e.CanHandle(u) {
			return e
		}
	}
	return nil
}

func (a *App) probeKind(ctx context.Context, u *url.URL) models.JobKind {
	if u == nil || (u.Scheme != "http" && u.Scheme != "https") {
		return models.JobKindUnknown
	}
	probeCtx, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(probeCtx, http.MethodHead, u.String(), nil)
	resp, err := a.downloadClient.HTTP.Do(req)
	if err == nil && resp != nil {
		resp.Body.Close()
		ct := strings.ToLower(resp.Header.Get("Content-Type"))
		if strings.Contains(ct, "text/html") || strings.Contains(ct, "application/xhtml") {
			return models.JobKindArticle
		}
		// Anything else is treated as a direct file (images, zips, binaries, etc.).
		return models.JobKindDirectFile
	}
	// If HEAD fails (some servers reject it), default to file; the downloader can still GET.
	return models.JobKindDirectFile
}

func (a *App) RetryFailed(ctx context.Context, maxParallel int) {
	ids := a.q.IDsWhere(func(j models.Job) bool { return j.State == models.StateError && j.Retryable })
	for _, id := range ids {
		// reset fields
		empty := ""
		zero := int64(0)
		a.patchJob(id, queue.Patch{
			State:        ptrState(models.StateQueued),
			ErrorSummary: &empty,
			ErrorDetails: &empty,
			BytesDone:    &zero,
		})
	}
	a.StartQueued(ctx, maxParallel)
}

func (a *App) DeleteCompleted() []models.JobID {
	removed := a.q.RemoveWhere(func(j models.Job) bool {
		return j.State == models.StateSuccess || j.State == models.StateCanceled
	})
	for _, id := range removed {
		a.emit(events.JobRemoved{Base: events.Base{T: time.Now()}, JobID: id})
	}
	a.emit(a.metricsEvent())
	return removed
}

func (a *App) ClearErrors() {
	ids := a.q.IDsWhere(func(j models.Job) bool { return j.State == models.StateError })
	for _, id := range ids {
		empty := ""
		retryable := false
		a.patchJob(id, queue.Patch{ErrorSummary: &empty, ErrorDetails: &empty, Retryable: &retryable})
	}
}

func (a *App) patchJob(id models.JobID, p queue.Patch) {
	j, ok := a.q.Patch(id, p)
	if !ok {
		return
	}
	ev := events.JobUpdated{Base: events.Base{T: time.Now()}, JobID: id}
	if p.State != nil {
		ev.State = p.State
	}
	if p.BytesDone != nil {
		ev.BytesDone = p.BytesDone
	}
	if p.BytesTotal != nil {
		ev.BytesTotal = p.BytesTotal
	}
	if p.SpeedBpsEMA != nil {
		ev.SpeedBpsEMA = p.SpeedBpsEMA
	}
	if p.Workers != nil {
		ev.Workers = p.Workers
	}
	if p.ErrorSummary != nil {
		ev.ErrorSummary = p.ErrorSummary
	}
	if p.ErrorDetails != nil {
		ev.ErrorDetails = p.ErrorDetails
	}
	if p.Retryable != nil {
		ev.Retryable = p.Retryable
	}
	if p.Attempts != nil {
		ev.Attempts = p.Attempts
	}
	if p.OutputPath != nil {
		ev.OutputPath = p.OutputPath
	}

	a.emit(ev)
	_ = j
	a.emit(a.metricsEvent())
}

func (a *App) metricsEvent() events.Metrics {
	snap := a.q.Snapshot()
	var live models.LiveMetrics
	for _, j := range snap {
		switch j.State {
		case models.StateQueued:
			live.QueuedJobs++
		case models.StateDownloading, models.StateRetrying:
			live.ActiveJobs++
		case models.StateSuccess:
			live.CompletedSuccess++
		case models.StateError:
			live.CompletedError++
		}
		if j.State == models.StateDownloading {
			live.BytesPerSecond += j.SpeedBpsEMA
			live.WorkersActive += j.Workers
		}
	}
	live.WorkersMax = 0
	live.Uptime = time.Since(a.startedAt)

	return events.Metrics{Base: events.Base{T: time.Now()}, Live: live}
}

func ptrState(s models.JobState) *models.JobState { return &s }
func ptrBool(b bool) *bool                       { return &b }
