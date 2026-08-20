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
