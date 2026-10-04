package config

import (
	consumerorchestrator "kafka-golang-analytics/internal/consumer_orchestrator"
	"kafka-golang-analytics/internal/logging"
	"kafka-golang-analytics/internal/storage"
	"os"
	"strconv"
	"strings"
)

type BaseConfig struct {
	Brokers []string              `json:"brokers,omitempty"`
	Logging logging.LoggingConfig `json:"logging,omitempty"`
}

type ProducerConfig struct {
	BaseConfig            `json:"base_config,omitempty"`
	ProduceTopic          string `json:"produce_topic,omitempty"`
	HttpAddr              string `json:"http_addr,omitempty"`
	HttpReadTimeout       int    `json:"read_timeout,omitempty"`
	HttpShutdownTimeout   int
	BufferDrainingTimeout int
}

type ConsumerConfig struct {
	BaseConfig              `json:"base_config,omitempty"`
	ConsumerOrchestrator    consumerorchestrator.ConsumerOrchestratorConfig `json:"consumer_orchestrator_config,omitempty"`
	DatabaseConfig          storage.DatabaseConfig                          `json:"database_config,omitempty"`
	ConsumerDrainingTimeout int                                             `json:"consumer_draining_timeout,omitempty"`
	ConsumerTopic           string                                          `json:"consumer_topic,omitempty"`
	GroupID                 string                                          `json:"group_id,omitempty"`
}

func LoadBaseConfig() BaseConfig {
	return BaseConfig{
		Brokers: strings.Split(envOrDefault("KAFKA_BROKERS", ""), ","),
		Logging: logging.LoggingConfig{
			Level: envOrDefault("LOG_LEVEL", "info"),
		},
	}
}

func LoadOrchestratorConfig() consumerorchestrator.ConsumerOrchestratorConfig {
	return consumerorchestrator.ConsumerOrchestratorConfig{
		ConsumerFlushTimeout: envAsInt("CONSUMER_FLUSH_TIMEOUT", 10),
	}
}

func LoadDatabaseConfig() storage.DatabaseConfig {
	return storage.DatabaseConfig{
		Dsn:               envOrDefault("POSTGRES_DSN", "postgres://analytics_chall@localhost:5432/analytics_db"),
		ConnectionTimeout: envAsInt("POSTGRES_CONNECTION_TIMEOUT", 30),
	}
}

func LoadProducerConfig() ProducerConfig {
	return ProducerConfig{
		BaseConfig:            LoadBaseConfig(),
		ProduceTopic:          envOrDefault("PRODUCER_TOPIC", "incoming.user_activity"),
		HttpReadTimeout:       envAsInt("HTTP_READ_TIMEOUT", 3),
		HttpShutdownTimeout:   envAsInt("HTTP_SHUTDOWN_TIMEOUT", 10),
		BufferDrainingTimeout: envAsInt("PRODUCER_BUFFER_DRAINING_TIMEOUT", 60),
	}
}

func LoadConsumerConfig() ConsumerConfig {
	return ConsumerConfig{
		BaseConfig:              LoadBaseConfig(),
		ConsumerOrchestrator:    LoadOrchestratorConfig(),
		DatabaseConfig:          LoadDatabaseConfig(),
		ConsumerDrainingTimeout: envAsInt("CONSUMER_DRAINING_TIMEOUT", 10),
		ConsumerTopic:           envOrDefault("CONSUMER_TOPIC", "incoming.user_activity"),
		GroupID:                 envOrDefault("KAFKA_GROUP_ID", "analytics-agg"),
	}
}

func envOrDefault(key, defaultValue string) string {
	value := os.Getenv(key)

	if value != "" {
		return value
	}

	return defaultValue
}

func envAsInt(key string, defaultVal int) int {
	valueStr := os.Getenv(key)
	if valueStr == "" {
		return defaultVal
	}

	value, err := strconv.Atoi(valueStr)
	if err != nil {
		return defaultVal
	}

	return value
}
