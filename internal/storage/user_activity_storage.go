package storage

import (
	"context"
	"database/sql"
	"fmt"
	"kafka-golang-analytics/internal/types"
	"sort"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	gormlogger "gorm.io/gorm/logger"
)

type DatabaseConfig struct {
	Dsn               string `json:"dsn,omitempty"`
	ConnectionTimeout int    `json:"connection_timeout,omitempty"`
	MaxOpenConns      int    `json:"max_open_conns,omitempty"`
}

type userActivityStat struct {
	UserID             string `gorm:"primaryKey"`
	ActivityType       string `gorm:"primaryKey"`
	TotalActivityCount int    `gorm:"not null"`
}

func (userActivityStat) TableName() string { return "user_activity_stats" }

type StatsStore struct {
	cfg DatabaseConfig
	db  *gorm.DB
}

func NewStatsStore(ctx context.Context, cfg DatabaseConfig) (*StatsStore, error) {
	db, err := gorm.Open(postgres.Open(cfg.Dsn), &gorm.Config{
		Logger: gormlogger.Default.LogMode(gormlogger.Silent),
	})
	if err != nil {
		return nil, fmt.Errorf("postgres open: %w", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("postgres pool: %w", err)
	}
	if cfg.MaxOpenConns > 0 {
		sqlDB.SetMaxOpenConns(cfg.MaxOpenConns)

		sqlDB.SetMaxIdleConns(cfg.MaxOpenConns)
	}
	if err := sqlDB.PingContext(ctx); err != nil {
		_ = sqlDB.Close()
		return nil, fmt.Errorf("postgres ping: %w", err)
	}
	if err := db.WithContext(ctx).AutoMigrate(&userActivityStat{}); err != nil {
		_ = sqlDB.Close()
		return nil, fmt.Errorf("migrate user_activity_stats: %w", err)
	}
	return &StatsStore{db: db}, nil
}

func (s *StatsStore) IncrementCounts(ctx context.Context, deltas map[types.Key]int) error {
	if len(deltas) == 0 {
		return nil
	}
	rows := make([]userActivityStat, 0, len(deltas))
	for k, n := range deltas {
		rows = append(rows, userActivityStat{
			UserID:             k.UserID,
			ActivityType:       k.ActivityType,
			TotalActivityCount: n,
		})
	}

	sort.Slice(rows, func(i, j int) bool {
		if rows[i].UserID != rows[j].UserID {
			return rows[i].UserID < rows[j].UserID
		}
		return rows[i].ActivityType < rows[j].ActivityType
	})

	err := s.db.WithContext(ctx).
		Clauses(clause.OnConflict{
			Columns: []clause.Column{{Name: "user_id"}, {Name: "activity_type"}},
			DoUpdates: clause.Assignments(map[string]interface{}{
				"total_activity_count": gorm.Expr(
					"user_activity_stats.total_activity_count + EXCLUDED.total_activity_count"),
			}),
		}).
		Create(&rows).Error

	if err != nil {
		return fmt.Errorf("increment user_activity_stats: %w", err)
	}
	return nil
}

type activityTotal struct {
	ActivityType string
	Total        int
}

func (s *StatsStore) GetStats(ctx context.Context, page, pageSize int) (types.Stats, error) {
	if page < 1 || pageSize < 1 {
		return types.Stats{}, fmt.Errorf("page and page size must be positive, got page=%d page_size=%d", page, pageSize)
	}

	var (
		totalUsers int64
		totals     []activityTotal
		pageRows   []userActivityStat
	)

	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&userActivityStat{}).Distinct("user_id").Count(&totalUsers).Error; err != nil {
			return fmt.Errorf("count users: %w", err)
		}

		err := tx.Model(&userActivityStat{}).
			Select("activity_type, SUM(total_activity_count)::bigint AS total").
			Group("activity_type").
			Scan(&totals).Error
		if err != nil {
			return fmt.Errorf("total per activity: %w", err)
		}

		usersOnPage := tx.Model(&userActivityStat{}).
			Distinct("user_id").
			Order("user_id").
			Limit(pageSize).
			Offset((page - 1) * pageSize)

		err = tx.Where("user_id IN (?)", usersOnPage).
			Order("user_id, activity_type").
			Find(&pageRows).Error
		if err != nil {
			return fmt.Errorf("users on page %d: %w", page, err)
		}

		return nil
	}, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return types.Stats{}, fmt.Errorf("read user_activity_stats: %w", err)
	}

	return buildStats(int(totalUsers), totals, pageRows, page, pageSize), nil
}

func buildStats(totalUsers int, totals []activityTotal, pageRows []userActivityStat, page, pageSize int) types.Stats {
	stats := types.Stats{
		TotalUsers:         totalUsers,
		ActivityTotals:     make(map[string]int, len(totals)),
		UserActivityCounts: make(map[string]map[string]int),
		Pagination: types.Pagination{
			Page:       page,
			PageSize:   pageSize,
			TotalPages: totalPages(totalUsers, pageSize),
		},
	}

	for _, total := range totals {
		stats.ActivityTotals[total.ActivityType] = total.Total
	}

	for _, row := range pageRows {
		perUser, ok := stats.UserActivityCounts[row.UserID]
		if !ok {
			perUser = make(map[string]int)
			stats.UserActivityCounts[row.UserID] = perUser
		}
		perUser[row.ActivityType] += row.TotalActivityCount
	}

	return stats
}

func totalPages(totalUsers, pageSize int) int {
	if pageSize < 1 {
		return 0
	}

	return (totalUsers + pageSize - 1) / pageSize
}

func (s *StatsStore) Close() {
	if sqlDB, err := s.db.DB(); err == nil {
		_ = sqlDB.Close()
	}
}
