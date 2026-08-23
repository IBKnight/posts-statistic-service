package httptransport

import (
	"fmt"
	"net/http"
)

type Handler struct {
}

func (h *Handler) InitRoutes() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /api/post_stat/{id}", h.getPostStatistic)
	mux.HandleFunc("GET /api/dump", h.getDump)
	mux.HandleFunc("GET /api/status", h.getStatus)

	return mux
}

func (h *Handler) getPostStatistic(w http.ResponseWriter, r *http.Request) {
	postID := r.PathValue("id")

	if postID == "" {
		//add error handling
		return
	}

	fmt.Println(postID)

}

func (h *Handler) getDump(w http.ResponseWriter, r *http.Request) {

}

func (h *Handler) getStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintln(w, "ok")
}
