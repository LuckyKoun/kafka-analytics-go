package config

import (
	"errors"
	consumerorchestrator "kafka-golang-analytics/internal/consumer_orchestrator"
	"kafka-golang-analytics/internal/logging"
	"kafka-golang-analytics/internal/storage"
	"os"
	"strconv"
	"strings"
)

var warnings = logging.New(logging.LoggingConfig{Level: "warn"})

type BaseConfig struct {
	Brokers []string              `json:"brokers,omitempty"`
	Logging logging.LoggingConfig `json:"logging,omitempty"`
}

func (c BaseConfig) Validate() error {
	if len(c.Brokers) == 0 {
		return errors.New("KAFKA_BROKERS must list at least one broker")
	}

	return nil
}

type ProducerConfig struct {
	BaseConfig            `json:"base_config,omitempty"`
	ProduceTopic          string `json:"produce_topic,omitempty"`
	HttpAddr              string `json:"http_addr,omitempty"`
	HttpReadTimeout       int    `json:"read_timeout,omitempty"`
	HttpShutdownTimeout   int
	BufferDrainingTimeout int
}

type APIConfig struct {
	Logging             logging.LoggingConfig  `json:"logging,omitempty"`
	DatabaseConfig      storage.DatabaseConfig `json:"database_config,omitempty"`
	HttpAddr            string                 `json:"http_addr,omitempty"`
	HttpReadTimeout     int                    `json:"read_timeout,omitempty"`
	HttpShutdownTimeout int
	QueryTimeout        int
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
		Brokers: parseBrokers(envOrDefault("KAFKA_BROKERS", "")),
		Logging: logging.LoggingConfig{
			Level: envOrDefault("LOG_LEVEL", "info"),
		},
	}
}

func LoadOrchestratorConfig() consumerorchestrator.ConsumerOrchestratorConfig {
	return consumerorchestrator.ConsumerOrchestratorConfig{
		ConsumerFlushTimeout: envAsInt("CONSUMER_FLUSH_TIMEOUT", 10),
		DrainingTimeout:      envAsInt("CONSUMER_DRAINING_TIMEOUT", 10),
		PartitionBuffer:      envAsInt("CONSUMER_PARTITION_BUFFER", 2),
		MaxPollRecords:       envAsInt("CONSUMER_MAX_POLL_RECORDS", 500),
	}
}

func LoadDatabaseConfig() storage.DatabaseConfig {
	return storage.DatabaseConfig{
		Dsn:               envOrDefault("POSTGRES_DSN", "postgres://analytics_chall@localhost:5432/analytics_db"),
		ConnectionTimeout: envAsInt("POSTGRES_CONNECTION_TIMEOUT", 30),
		MaxOpenConns:      envAsInt("POSTGRES_MAX_OPEN_CONNS", 10),
	}
}

func LoadProducerConfig() ProducerConfig {
	return ProducerConfig{
		BaseConfig:            LoadBaseConfig(),
		ProduceTopic:          envOrDefault("PRODUCER_TOPIC", "incoming.user_activity"),
		HttpAddr:              envOrDefault("PRODUCER_HTTP_ADDR", ":8081"),
		HttpReadTimeout:       envAsInt("HTTP_READ_TIMEOUT", 3),
		HttpShutdownTimeout:   envAsInt("HTTP_SHUTDOWN_TIMEOUT", 10),
		BufferDrainingTimeout: envAsInt("PRODUCER_BUFFER_DRAINING_TIMEOUT", 60),
	}
}

func LoadAPIConfig() APIConfig {
	return APIConfig{
		Logging:             LoadBaseConfig().Logging,
		DatabaseConfig:      LoadDatabaseConfig(),
		HttpAddr:            envOrDefault("HTTP_ADDR", ":8080"),
		HttpReadTimeout:     envAsInt("HTTP_READ_TIMEOUT", 3),
		HttpShutdownTimeout: envAsInt("HTTP_SHUTDOWN_TIMEOUT", 10),
		QueryTimeout:        envAsInt("API_QUERY_TIMEOUT", 5),
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

func parseBrokers(list string) []string {
	var brokers []string

	for _, broker := range strings.Split(list, ",") {
		if broker = strings.TrimSpace(broker); broker != "" {
			brokers = append(brokers, broker)
		}
	}

	return brokers
}

func envOrDefault(key, defaultValue string) string {
	value := strings.TrimSpace(os.Getenv(key))

	if value != "" {
		return value
	}

	return defaultValue
}

func envAsInt(key string, defaultVal int) int {
	valueStr := envOrDefault(key, "")
	if valueStr == "" {
		return defaultVal
	}

	value, err := strconv.Atoi(valueStr)
	if err != nil {
		warnings.Warn("environment variable is not a whole number, using the default",
			"variable", key, "value", valueStr, "default", defaultVal)
		return defaultVal
	}

	return value
}
