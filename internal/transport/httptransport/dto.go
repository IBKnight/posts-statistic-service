package httptransport

type PostStatisticResponse struct {
	PostID      int64 `json:"postId"`
	Views       int64 `json:"views"`
	UniqueViews int64 `json:"uniqueViews"`
	Likes       int64 `json:"likes"`
	Shares      int64 `json:"shares"`
	Reports     int64 `json:"reports"`
}
