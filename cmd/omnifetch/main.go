package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"omnifetch/internal/engine"
	"omnifetch/internal/events"
	"omnifetch/internal/models"
	"omnifetch/internal/utils"
	"omnifetch/internal/version"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	args := os.Args[1:]
	if len(args) > 0 {
		switch strings.ToLower(args[0]) {
		case "-h", "--help", "help", "-help", "--h":
			printHelp()
			return
		case "-v", "--version", "version":
			fmt.Println("omnifetch " + version.Current)
			return
		}
	}

	printBanner(os.Stdout)

	// Important: don't spam stderr while the TUI is running (it corrupts the screen).
	// Default to a local log file; allow opt-in terminal logs via env.
	var logOut io.Writer = io.Discard
	if os.Getenv("OMNIFETCH_DEBUG_TERMINAL") == "1" {
		logOut = os.Stderr
	} else {
		_ = os.MkdirAll("downloads", 0o755)
		if f, err := os.OpenFile(filepath.Join("downloads", "omnifetch.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644); err == nil {
			defer f.Close()
			logOut = f
		}
	}
	log := slog.New(slog.NewJSONHandler(logOut, &slog.HandlerOptions{Level: slog.LevelInfo}))

	// Basic flags (no config files, no env gymnastics).
	fs := flag.NewFlagSet("omnifetch", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	outDir := fs.String("out", "downloads", "output directory")
	parallel := fs.Int("j", 3, "max parallel jobs")
	_ = fs.Parse(args)
	urlArgs := fs.Args()

	app, err := engine.NewApp(engine.Config{
		Log:               log,
		DefaultOutputDir:  *outDir,
		ProgressTickEvery: 120 * time.Millisecond,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer func() {
		_ = app.Close()
	}()

	if len(urlArgs) == 0 {
		u := promptOneURL(ctx, os.Stdin, os.Stdout)
		if u == "" {
			return
		}
		urlArgs = []string{u}
	}

	for _, u := range urlArgs {
		if _, err := app.AddURL(u); err == nil {
			continue
		} else {
			fmt.Fprintln(os.Stderr, "skip:", err)
		}
	}

	app.StartQueued(ctx, *parallel)

	exit := runCLI(ctx, app)
	os.Exit(exit)
}

func runCLI(ctx context.Context, app *engine.App) int {
	// Render loop: consume events and print a compact dashboard that won't flood the terminal.
	// We print progress at most every ~250ms regardless of event rate.
	type state struct {
		jobs    map[models.JobID]models.Job
		metrics models.LiveMetrics
	}
	st := state{jobs: make(map[models.JobID]models.Job)}

	lastPrint := time.Time{}
	printNow := func(force bool) {
		if !force && time.Since(lastPrint) < 250*time.Millisecond {
			return
		}
		lastPrint = time.Now()

		// Clear + redraw. Keep it simple and compatible.
		fmt.Print("\033[H\033[2J")
		fmt.Printf("OmniFetch (CLI) by debasis • uptime %s • throughput %s\n",
			utils.HumanDur(st.metrics.Uptime),
			utils.HumanRate(st.metrics.BytesPerSecond),
		)
		fmt.Printf("active=%d queued=%d ok=%d err=%d\n\n",
			st.metrics.ActiveJobs, st.metrics.QueuedJobs, st.metrics.CompletedSuccess, st.metrics.CompletedError,
		)

		// Show up to N jobs.
		const maxRows = 18
		snap := app.Snapshot()
		i := 0
		for _, j := range snap {
			if i >= maxRows {
				fmt.Printf("… %d more\n", len(snap)-maxRows)
				break
			}
			i++
			pct := ""
			spinner := ""
			if j.State == models.StateDownloading || j.State == models.StateRetrying {
				spinner = progressSpinner(time.Now())
			}
			stateLabel := j.State.String()
			if spinner != "" && j.BytesDone == 0 {
				stateLabel = stateLabel + " " + spinner
			}
			if j.BytesTotal > 0 && j.BytesDone > 0 {
				pct = fmt.Sprintf(" %.1f%%", 100*float64(j.BytesDone)/float64(j.BytesTotal))
			} else if spinner != "" {
				pct = " starting " + spinner
			}
			errPart := ""
			if j.State == models.StateError && j.ErrorSummary != "" {
				errPart = " — " + j.ErrorSummary
			}
			fmt.Printf("[%s] %-7s %s%s • %s%s\n",
				stateLabel,
				j.Kind.String(),
				utils.HumanBytes(j.BytesDone),
				func() string {
					if j.BytesTotal > 0 {
						return "/" + utils.HumanBytes(j.BytesTotal)
					}
					return ""
				}(),
				utils.HumanRate(j.SpeedBpsEMA),
				pct+errPart,
			)
			fmt.Printf("  %s\n", j.URL.String())
			if j.OutputPath != "" && (j.State == models.StateSuccess) {
				fmt.Printf("  -> %s\n", j.OutputPath)
			}
		}
	}

	doneTick := time.NewTicker(300 * time.Millisecond)
	defer doneTick.Stop()

	for {
		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.Canceled) {
				fmt.Fprintln(os.Stderr, "\nCanceled.")
			}
			return 130
		case <-doneTick.C:
			// refresh even if no events (throughput EMA looks nicer)
			printNow(false)
			if app.Done() {
				printNow(true)
				return exitCode(app.Snapshot())
			}
		case ev, ok := <-app.Events():
			if !ok {
				return exitCode(app.Snapshot())
			}
			switch e := ev.(type) {
			case events.JobAdded:
				st.jobs[e.Job.ID] = e.Job
			case events.JobUpdated:
				j := st.jobs[e.JobID]
				if e.State != nil {
					j.State = *e.State
				}
				if e.BytesDone != nil {
					j.BytesDone = *e.BytesDone
				}
				if e.BytesTotal != nil {
					j.BytesTotal = *e.BytesTotal
				}
				if e.SpeedBpsEMA != nil {
					j.SpeedBpsEMA = *e.SpeedBpsEMA
				}
				if e.Workers != nil {
					j.Workers = *e.Workers
				}
				if e.ErrorSummary != nil {
					j.ErrorSummary = *e.ErrorSummary
				}
				if e.ErrorDetails != nil {
					j.ErrorDetails = *e.ErrorDetails
				}
				if e.Retryable != nil {
					j.Retryable = *e.Retryable
				}
				if e.Attempts != nil {
					j.Attempts = *e.Attempts
				}
				if e.OutputPath != nil {
					j.OutputPath = *e.OutputPath
				}
				st.jobs[e.JobID] = j
			case events.Metrics:
				st.metrics = e.Live
			}
			printNow(false)
		}
	}
}

func exitCode(jobs []models.Job) int {
	for _, j := range jobs {
		if j.State == models.StateError {
			return 2
		}
	}
	return 0
}

func progressSpinner(now time.Time) string {
	frames := []string{"|", "/", "-", "\\"}
	idx := int(now.UnixNano() / (120 * int64(time.Millisecond)))
	return frames[idx%len(frames)]
}

func promptOneURL(ctx context.Context, in io.Reader, out io.Writer) string {
	fmt.Fprintln(out, "Enter the URL to download:")
	fmt.Fprintln(out)

	lines := make(chan string, 8)
	done := make(chan struct{})

	go func() {
		defer close(done)
		sc := bufio.NewScanner(in)
		// Allow long URLs.
		buf := make([]byte, 0, 64*1024)
		sc.Buffer(buf, 2*1024*1024)
		for sc.Scan() {
			lines <- sc.Text()
		}
		if err := sc.Err(); err != nil {
			lines <- "__OMNIFETCH_SCAN_ERR__" + err.Error()
		}
	}()

	for {
		fmt.Fprint(out, "URL: ")
		select {
		case <-ctx.Done():
			fmt.Fprintln(out)
			return ""
		case <-done:
			return ""
		case ln := <-lines:
			if strings.HasPrefix(ln, "__OMNIFETCH_SCAN_ERR__") {
				fmt.Fprintln(out, "")
				fmt.Fprintln(out, "Input error:", strings.TrimPrefix(ln, "__OMNIFETCH_SCAN_ERR__"))
				return ""
			}
			line := strings.TrimSpace(ln)
			if line == "" {
				return ""
			}
			return line
		}
	}
}

func printHelp() {
	fmt.Print(`OmniFetch — keyless public-content downloader (CLI)

Usage:
  omnifetch -out downloads -j 3 <url>...
  omnifetch -h|--help

Flags:
  -out <dir>   output directory (default: downloads)
  -j <n>       max parallel jobs (default: 3)

Notes:
  - Public-only. No logins. No DRM bypass.
  - Supports direct files over HTTP(S), public article pages, and media hosts handled by yt-dlp.
  - Media uses yt-dlp via go-ytdlp (first use may download yt-dlp/ffmpeg).
`)
}

func printBanner(out io.Writer) {
	b, err := os.ReadFile("assets/banner.txt")
	if err != nil {
		return
	}
	fmt.Fprintln(out, string(b))
	if len(b) > 0 && b[len(b)-1] != '\n' {
		fmt.Fprintln(out)
	}
}
