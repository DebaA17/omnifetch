package engine

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"

	omnierr "omnifetch/internal/errors"
	"omnifetch/internal/events"
	"omnifetch/internal/models"
	"omnifetch/internal/utils"
)

type ArticleEngine struct {
	log  *slog.Logger
	out  string
	http *http.Client
}

func NewArticleEngine(log *slog.Logger, out string) *ArticleEngine {
	if log == nil {
		log = slog.Default()
	}
	return &ArticleEngine{
		log: log,
		out: out,
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

func (*ArticleEngine) Kind() models.JobKind { return models.JobKindArticle }

func (*ArticleEngine) CanHandle(u *url.URL) bool {
	if u == nil {
		return false
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return false
	}
	return true
}

func (e *ArticleEngine) Run(ctx context.Context, job models.Job, emit EmitFunc) (string, error) {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, job.URL.String(), nil)
	req.Header.Set("User-Agent", "OmniFetch/1.0 (+public-content-downloader)")
	resp, err := e.http.Do(req)
	if err != nil {
		return "", omnierr.ClassifyNet("GET", job.URL.String(), err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return "", omnierr.New(omnierr.CodeHTTP, "Server returned an error.", omnierr.HTTPStatus(resp.StatusCode), omnierr.Details(resp.Status), omnierr.Retryable(resp.StatusCode >= 500 || resp.StatusCode == 429))
	}

	ct := strings.ToLower(resp.Header.Get("Content-Type"))
	if !strings.Contains(ct, "text/html") && !strings.Contains(ct, "application/xhtml") {
		return "", omnierr.New(omnierr.CodeUnsupportedDomain, "This link doesn’t look like an article page.", omnierr.Details("content-type: "+ct), omnierr.Retryable(false), omnierr.Sev(omnierr.SeverityWarn))
	}

	// Limit memory: parse via reader, but buffer enough for charset sniffing is handled by goquery.
	doc, err := goquery.NewDocumentFromReader(resp.Body)
	if err != nil {
		return "", omnierr.Wrap(omnierr.CodeUnknown, "Failed to parse article HTML.", err, omnierr.Retryable(false))
	}

	cleanDoc(doc)
	md, title := extractMarkdown(doc)
	if strings.TrimSpace(md) == "" {
		return "", omnierr.New(omnierr.CodeUnknown, "No readable article content found.", omnierr.Retryable(false), omnierr.Sev(omnierr.SeverityWarn))
	}

	slug := slugify(title)
	if slug == "" {
		slug = "article-" + utils.RandHex(4)
	}
	outPath := filepath.Join(e.out, slug+".md")
	if err := os.MkdirAll(e.out, 0o755); err != nil {
		return "", omnierr.Wrap(omnierr.CodePermissionDenied, "Cannot create output directory.", err, omnierr.Sev(omnierr.SeverityFatal))
	}

	tmp := outPath + ".tmp"
	if err := os.WriteFile(tmp, []byte(md), 0o644); err != nil {
		return "", omnierr.Wrap(omnierr.CodePermissionDenied, "Cannot write article file.", err, omnierr.Sev(omnierr.SeverityFatal))
	}
	if err := os.Rename(tmp, outPath); err != nil {
		return "", omnierr.Wrap(omnierr.CodePermissionDenied, "Failed to finalize article file.", err, omnierr.Sev(omnierr.SeverityFatal))
	}

	emit(events.JobUpdated{Base: events.Base{T: time.Now()}, JobID: job.ID, OutputPath: ptrStr(outPath)})
	return outPath, nil
}

func cleanDoc(doc *goquery.Document) {
	// Remove obvious noise.
	doc.Find("script, style, nav, footer, header, aside, noscript, iframe").Each(func(_ int, s *goquery.Selection) {
		s.Remove()
	})
	doc.Find("[role='navigation'], [aria-hidden='true']").Each(func(_ int, s *goquery.Selection) {
		s.Remove()
	})
	doc.Find("[class*='cookie'], [id*='cookie'], [class*='ad'], [id*='ad'], [class*='promo'], [id*='promo']").Each(func(_ int, s *goquery.Selection) {
		s.Remove()
	})
}

func extractMarkdown(doc *goquery.Document) (string, string) {
	title := strings.TrimSpace(doc.Find("title").First().Text())
	h1 := strings.TrimSpace(doc.Find("h1").First().Text())
	if h1 != "" {
		title = h1
	}

	var buf bytes.Buffer
	w := bufio.NewWriter(&buf)
	if title != "" {
		fmt.Fprintf(w, "# %s\n\n", normalizeSpace(title))
	}

	body := doc.Find("body").First()
	if body.Length() == 0 {
		body = doc.Selection
	}

	// Prefer main/article if present.
	root := body.Find("main, article").First()
	if root.Length() == 0 {
		root = body
	}

	writeNodesMarkdown(w, root)
	_ = w.Flush()
	return strings.TrimSpace(buf.String()) + "\n", title
}

func writeNodesMarkdown(w io.Writer, root *goquery.Selection) {
	root.Find("h1, h2, h3, p, pre, code, ul, ol, li, blockquote").Each(func(_ int, s *goquery.Selection) {
		name := goquery.NodeName(s)
		text := normalizeSpace(s.Text())
		if text == "" && (name == "p" || name == "li" || name == "blockquote") {
			return
		}
		switch name {
		case "h1":
			fmt.Fprintf(w, "## %s\n\n", text)
		case "h2":
			fmt.Fprintf(w, "## %s\n\n", text)
		case "h3":
			fmt.Fprintf(w, "### %s\n\n", text)
		case "p":
			fmt.Fprintf(w, "%s\n\n", text)
		case "blockquote":
			lines := strings.Split(text, "\n")
			for _, ln := range lines {
				ln = strings.TrimSpace(ln)
				if ln == "" {
					continue
				}
				fmt.Fprintf(w, "> %s\n", ln)
			}
			fmt.Fprintln(w)
		case "pre":
			code := strings.TrimSpace(s.Text())
			if code == "" {
				return
			}
			fmt.Fprintf(w, "```\n%s\n```\n\n", code)
		case "ul", "ol":
			// handled by li iteration; skip container.
		case "li":
			fmt.Fprintf(w, "- %s\n", text)
		case "code":
			// Inline code is usually captured by parent; skip.
		}
	})
}

var nonWord = regexp.MustCompile(`[^a-z0-9\- ]+`)
var multiDash = regexp.MustCompile(`[\s\-]+`)

func slugify(s string) string {
	s = strings.ToLower(normalizeSpace(s))
	s = nonWord.ReplaceAllString(s, "")
	s = multiDash.ReplaceAllString(s, "-")
	s = strings.Trim(s, "-")
	if len(s) > 64 {
		s = s[:64]
		s = strings.Trim(s, "-")
	}
	return s
}

func normalizeSpace(s string) string {
	return strings.Join(strings.Fields(strings.TrimSpace(s)), " ")
}

var _ Runner = (*ArticleEngine)(nil)

