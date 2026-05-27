<h1 align="center">OmniFetch</h1>

<p align="center">
	<a href="https://github.com/debaa17/omnifetch/actions/workflows/release.yml">
		<img src="https://github.com/debaa17/omnifetch/actions/workflows/release.yml/badge.svg" alt="Release status" />
	</a>
	<a href="https://github.com/debaa17/omnifetch/blob/main/LICENSE">
		<img src="https://img.shields.io/badge/license-MIT-blue.svg" alt="MIT license" />
	</a>
</p>

OmniFetch is a Go-based CLI downloader for **publicly accessible** content:

- Direct files over HTTP(S) (segmented, concurrent, resumable range downloading)
- Public articles (extracts clean Markdown)
- Public media sources via `yt-dlp` (through `go-ytdlp`)

The codebase is organized around a small set of focused packages:

- `internal/engine`: job routing and orchestration
- `internal/downloader`: high-performance segmented HTTP downloader
- `internal/queue`, `internal/events`, `internal/errors`, `internal/models`: domain model

## Install

### Homebrew

```bash
brew tap DebaA17/tap
brew install omnifetch
```

### Snap

```bash
sudo snap install omnifetch
```

## Usage

```bash
omnifetch -out downloads -j 3 <url>...
omnifetch -h
omnifetch https://example.com/file.zip
```

## Requirements

- Go `1.25+` (required by `github.com/lrstanley/go-ytdlp`)
- Network access for first run if `yt-dlp` / `ffmpeg` are not installed (go-ytdlp can download them into its cache).

## Notes

OmniFetch does not attempt to bypass authentication, DRM, private content controls, or legal protections.
