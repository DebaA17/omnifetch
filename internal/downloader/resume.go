package downloader

import (
	"encoding/json"
	"os"
	"path/filepath"
	"time"
)

type resumeState struct {
	Version      int       `json:"version"`
	URL          string    `json:"url"`
	OutputPath   string    `json:"output_path"`
	TempPath     string    `json:"temp_path"`
	MetaPath     string    `json:"meta_path"`
	Size         int64     `json:"size"`
	ETag         string    `json:"etag,omitempty"`
	LastModified string    `json:"last_modified,omitempty"`
	ChunkSize    int64     `json:"chunk_size"`
	Complete     []bool    `json:"complete"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

func resumePaths(outputPath, tempDir string) (tmpPath, metaPath string) {
	base := filepath.Base(outputPath)
	dir := tempDir
	if dir == "" {
		dir = filepath.Join(filepath.Dir(outputPath), ".omnifetch.partial")
	}
	_ = os.MkdirAll(dir, 0o755)
	tmpPath = filepath.Join(dir, base+".part")
	metaPath = filepath.Join(dir, base+".json")
	return tmpPath, metaPath
}

func loadResume(metaPath string) (*resumeState, error) {
	b, err := os.ReadFile(metaPath)
	if err != nil {
		return nil, err
	}
	var st resumeState
	if err := json.Unmarshal(b, &st); err != nil {
		return nil, err
	}
	return &st, nil
}

func saveResumeAtomic(metaPath string, st *resumeState) error {
	st.UpdatedAt = time.Now()
	b, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}
	tmp := metaPath + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, metaPath)
}

