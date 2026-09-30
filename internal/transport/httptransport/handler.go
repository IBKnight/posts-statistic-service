package httptransport

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/IBKnight/posts-statistic-service/internal/storage"
)

// maxBatchIDs caps GET /api/post_stats so one query string can't force a
// pass over an unbounded number of ids.
const maxBatchIDs = 500

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

// InitPublicRoutes serves what external clients and k8s probes need. It is
// safe to expose behind an Ingress/Service.
func (h *Handler) InitPublicRoutes() *http.ServeMux {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /api/post_stat/{id}", h.getPostStatistic)
	mux.HandleFunc("GET /api/post_stats", h.getPostStats)
	mux.HandleFunc("GET /healthz", h.getLive)
	mux.HandleFunc("GET /readyz", h.getReady)

	return mux
}

// InitInternalRoutes serves operational endpoints only: /api/dump and
// /api/status both snapshot the whole shard and take a lock across every
// bucket, so they must not sit on a publicly reachable port. Callers mount
// this on a separate, cluster-internal port (see cmd/shard wiring), typically
// alongside /metrics.
func (h *Handler) InitInternalRoutes() *http.ServeMux {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /api/dump", h.getDump)
	mux.HandleFunc("GET /api/status", h.getStatus)

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

// getPostStats is the batch counterpart of getPostStatistic: GET
// /api/post_stats?ids=1,2,3. Ids that don't belong to this shard, or that
// don't exist, are silently omitted from the response — same as GetMany.
func (h *Handler) getPostStats(w http.ResponseWriter, r *http.Request) {
	raw := r.URL.Query().Get("ids")
	if raw == "" {
		writeError(w, http.StatusBadRequest, "ids query parameter is required")
		return
	}

	parts := strings.Split(raw, ",")
	if len(parts) > maxBatchIDs {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("too many ids: max %d per request", maxBatchIDs))
		return
	}

	ids := make([]int64, 0, len(parts))
	for _, p := range parts {
		id, err := strconv.ParseInt(strings.TrimSpace(p), 10, 64)
		if err != nil || id <= 0 {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid post id %q", strings.TrimSpace(p)))
			return
		}
		ids = append(ids, id)
	}

	if ready, _ := h.readiness.Ready(); !ready {
		writeError(w, http.StatusServiceUnavailable, "shard is still catching up")
		return
	}

	found := h.store.GetMany(ids)
	posts := make([]postStatResponse, 0, len(found))

	for _, id := range ids {
		stats, ok := found[id]
		if !ok {
			continue
		}

		posts = append(posts, postStatResponse{
			PostID:  id,
			Views:   stats.Views,
			Likes:   stats.Likes,
			Shares:  stats.Shares,
			Reports: stats.Reports,
		})
	}

	writeJSON(w, http.StatusOK, postStatsResponse{
		Count: len(posts),
		Posts: posts,
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
