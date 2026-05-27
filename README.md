<h1 align="center">OmniFetch</h1>

<p align="center">
	<a href="https://github.com/debaa17/omnifetch/actions/workflows/release.yml">
		<img src="https://github.com/debaa17/omnifetch/actions/workflows/release.yml/badge.svg" alt="Release status" />
	</a>
</p>

OmniFetch is a CLI downloader for **publicly accessible** content:

- Direct files over HTTP(S) (segmented, concurrent, resumable range downloading)
- Public articles (extracts clean Markdown)
- Public media sources via `yt-dlp` (through `go-ytdlp`)

This repository is generated as a production-ready foundation with strict separation between:

- `internal/engine`: job routing and orchestration
- `internal/downloader`: high-performance segmented HTTP downloader
- `internal/queue`, `internal/events`, `internal/errors`, `internal/models`: domain model

## Build & Run

```bash
go env -w GOTOOLCHAIN=auto
go build ./cmd/omnifetch
./omnifetch -out downloads -j 3 <url>...
```

### CLI help / non-interactive add

```bash
./omnifetch -h
./omnifetch https://example.com/file.zip
```


### Docker

```bash
docker pull ghcr.io/debaa17/omnifetch:latest
docker run --rm ghcr.io/debaa17/omnifetch:latest --version
```

### Snap

```bash
sudo snap install omnifetch
```

### Tooling prerequisites

- Go `1.25+` (required by `github.com/lrstanley/go-ytdlp`)
- Network access for first run if `yt-dlp` / `ffmpeg` are not installed (go-ytdlp can download them into its cache).

## Notes (Public-only)

OmniFetch does not attempt to bypass authentication, DRM, private content controls, or legal protections.
