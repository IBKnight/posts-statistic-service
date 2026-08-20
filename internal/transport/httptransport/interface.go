package httptransport

import "github.com/IBKnight/posts-statistic-service/internal/domain"

type CalculationService interface {
	CalculatePostStatistic(postID int) (domain.PostStatistic, error)
}
