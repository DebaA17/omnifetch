package queue

import (
	"testing"

	"omnifetch/internal/models"
)

func TestQueueAddGetPatchRemove(t *testing.T) {
	t.Parallel()
	q := New()

	j := models.Job{ID: "a1", State: models.StateQueued}
	q.Add(j)
	if _, ok := q.Get("a1"); !ok {
		t.Fatalf("expected job to exist")
	}

	done := int64(123)
	state := models.StateDownloading
	_, ok := q.Patch("a1", Patch{BytesDone: &done, State: &state})
	if !ok {
		t.Fatalf("patch failed")
	}
	got, _ := q.Get("a1")
	if got.BytesDone != 123 || got.State != models.StateDownloading {
		t.Fatalf("got=%+v", got)
	}

	removed := q.RemoveWhere(func(j models.Job) bool { return j.State == models.StateDownloading })
	if len(removed) != 1 || removed[0] != "a1" {
		t.Fatalf("removed=%v", removed)
	}
	if _, ok := q.Get("a1"); ok {
		t.Fatalf("expected removed job to be gone")
	}
}

