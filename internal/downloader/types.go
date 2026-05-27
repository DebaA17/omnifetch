package downloader

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/url"
	"os"
	"time"
)

type Request struct {
	URL        *url.URL
	OutputPath string

	// TempDir is where partial files and metadata are stored. If empty, a
	// directory adjacent to OutputPath is used.
	TempDir string

	MaxWorkers int
	MinWorkers int

	// ChunkTarget is the desired time per chunk (used to derive chunk sizes).
	ChunkTarget time.Duration

	// If set, verifies the final file checksum.
	ExpectedSHA256 string
}

type Progress struct {
	BytesDone  int64
	BytesTotal int64
	Bps        float64
	Workers    int
}

type Reporter interface {
	Progress(Progress)
	Logf(level string, format string, args ...any)
}

type NopReporter struct{}

func (NopReporter) Progress(Progress)                          {}
func (NopReporter) Logf(string, string, ...any)                {}

type Result struct {
	OutputPath string
	Bytes      int64
	SHA256     string
}

type Downloader struct {
	client *Client
}

func New(c *Client) *Downloader {
	return &Downloader{client: c}
}

func sha256File(path string) (string, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(h.Sum(nil)), n, nil
}

func (d *Downloader) Download(ctx context.Context, req Request, r Reporter) (Result, error) {
	if r == nil {
		r = NopReporter{}
	}
	return d.downloadSegmented(ctx, req, r)
}

