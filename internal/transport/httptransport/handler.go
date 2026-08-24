package httptransport

import (
	"net/http"
	"strconv"

	"github.com/IBKnight/posts-statistic-service/internal/storage"
)

type Handler struct {
	store     Store
	readiness Readiness
}

func NewHandler(store Store, readiness Readiness) *Handler {
	if readiness == nil {
		readiness = AlwaysReady()
	}

	return &Handler{
		store:     store,
		readiness: readiness,
	}
}

func (h *Handler) InitRoutes() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /api/post_stat/{id}", h.getPostStatistic)
	mux.HandleFunc("GET /api/dump", h.getDump)
	mux.HandleFunc("GET /api/status", h.getStatus)
	mux.HandleFunc("GET /healthz", h.getLive)
	mux.HandleFunc("GET /readyz", h.getReady)

	return mux
}

func (h *Handler) getPostStatistic(w http.ResponseWriter, r *http.Request) {
	raw := r.PathValue("id")

	postID, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || postID <= 0 {
		writeError(w, http.StatusBadRequest, "post id must be a positive integer")
		return
	}

	if !h.store.Owns(postID) {
		w.Header().Set("X-Correct-Shard", strconv.Itoa(storage.ShardForPostID(postID)))
		writeError(w, http.StatusMisdirectedRequest, "post id belongs to another shard")
		return
	}

	if ready, _ := h.readiness.Ready(); !ready {
		writeError(w, http.StatusServiceUnavailable, "shard is still catching up")
		return
	}

	stats, ok := h.store.Get(postID)
	if !ok {
		writeError(w, http.StatusNotFound, "post not found")
		return
	}

	writeJSON(w, http.StatusOK, postStatResponse{
		PostID:  postID,
		Views:   stats.Views,
		Likes:   stats.Likes,
		Shares:  stats.Shares,
		Reports: stats.Reports,
	})
}

func (h *Handler) getDump(w http.ResponseWriter, r *http.Request) {
	if ready, _ := h.readiness.Ready(); !ready {
		writeError(w, http.StatusServiceUnavailable, "shard is still catching up")
		return
	}

	slots := h.store.Snapshot(nil)
	baseID := h.store.BaseID()

	posts := make([]postStatResponse, 0, 1024)

	for i, slot := range slots {
		if !slot.Exists {
			continue
		}

		posts = append(posts, postStatResponse{
			PostID:  baseID + int64(i),
			Views:   slot.Views,
			Likes:   slot.Likes,
			Shares:  slot.Shares,
			Reports: slot.Reports,
		})
	}

	writeJSON(w, http.StatusOK, dumpResponse{
		BaseID: baseID,
		MaxID:  h.store.MaxID(),
		Count:  len(posts),
		Posts:  posts,
	})
}

func (h *Handler) getStatus(w http.ResponseWriter, r *http.Request) {
	ready, lag := h.readiness.Ready()

	writeJSON(w, http.StatusOK, statusResponse{
		Ready:  ready,
		Lag:    lag,
		BaseID: h.store.BaseID(),
		MaxID:  h.store.MaxID(),
		Posts:  h.store.Count(),
	})
}

func (h *Handler) getLive(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok\n"))
}

func (h *Handler) getReady(w http.ResponseWriter, r *http.Request) {
	ready, _ := h.readiness.Ready()

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")

	if !ready {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte("catching up\n"))
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ready\n"))
}
