package storage

import (
	"context"
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
	// MaxOpenConns caps the connection pool; 0 keeps the database/sql default
	// (unlimited). Concurrent partition workers each flush on their own
	// connection, so size it against Postgres max_connections.
	MaxOpenConns int `json:"max_open_conns,omitempty"`
}

type userActivityStat struct {
	UserID             string `gorm:"primaryKey"`
	ActivityType       string `gorm:"primaryKey"`
	TotalActivityCount int    `gorm:"not null"`
}

func (userActivityStat) TableName() string { return "user_activity_stats" }

// StatsStore is safe for concurrent use: it only holds the *gorm.DB (a
// goroutine-safe pool), and IncrementCounts is a single additive upsert.
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
		// database/sql keeps only 2 idle connections by default, which would
		// make concurrent workers reconnect on every flush.
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

// IncrementCounts adds deltas to the stored totals. Rows are sorted so that
// concurrent callers lock overlapping keys in the same order, which avoids
// deadlocks between workers.
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

// Close releases the underlying connection pool.
func (s *StatsStore) Close() {
	if sqlDB, err := s.db.DB(); err == nil {
		_ = sqlDB.Close()
	}
}
