package useractivity

import (
	"kafka-golang-analytics/internal/types"
	"sync"
)

type UserActivityAnalysisService struct {
	mu     sync.Mutex
	counts map[types.Key]int
}

func (u *UserActivityAnalysisService) Add(activity types.UserActivity) {
	u.mu.Lock()
	defer u.mu.Unlock()

	key := types.Key{
		UserID:       activity.UserID,
		ActivityType: string(activity.ActivityType),
	}

	u.counts[key]++
}

func (u *UserActivityAnalysisService) Drain() map[types.Key]int {
	u.mu.Lock()
	defer u.mu.Unlock()

	deltas := u.counts
	u.counts = make(map[types.Key]int)

	return deltas
}

func (u *UserActivityAnalysisService) Restore(deltas map[types.Key]int) {
	u.mu.Lock()
	defer u.mu.Unlock()

	for key, count := range deltas {
		u.counts[key] += count
	}
}

func NewAnalysisService() *UserActivityAnalysisService {
	return &UserActivityAnalysisService{
		counts: make(map[types.Key]int),
	}
}
