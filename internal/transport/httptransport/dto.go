package httptransport

type postStatResponse struct {
	PostID  int64  `json:"post_id"`
	Views   uint32 `json:"views"`
	Likes   uint32 `json:"likes"`
	Shares  uint32 `json:"shares"`
	Reports uint32 `json:"reports"`
}

type dumpResponse struct {
	BaseID int64              `json:"base_id"`
	MaxID  int64              `json:"max_id"`
	Count  int                `json:"count"`
	Posts  []postStatResponse `json:"posts"`
}

type statusResponse struct {
	Ready  bool  `json:"ready"`
	Lag    int64 `json:"lag"`
	BaseID int64 `json:"base_id"`
	MaxID  int64 `json:"max_id"`
	Posts  int   `json:"posts"`
}
