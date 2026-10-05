package types

import "time"

type ActivityType string

const ActivityPageView ActivityType = "page_view"

type Metadata struct {
	PageUrl  string `json:"page_url,omitempty"`
	Referrer string `json:"referrer,omitempty"`
}

type UserActivity struct {
	UserID       string       `json:"user_id,omitempty"`
	ActivityType ActivityType `json:"activity_type,omitempty"`
	Timestamp    time.Time    `json:"timestamp,omitempty"`
	Metadata     Metadata     `json:"metadata,omitempty"`
}

type Key struct {
	UserID       string `json:"user_id,omitempty"`
	ActivityType string `json:"activity_type,omitempty"`
}
