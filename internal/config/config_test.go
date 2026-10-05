package config

import (
	"bytes"
	consumerorchestrator "kafka-golang-analytics/internal/consumer_orchestrator"
	"kafka-golang-analytics/internal/logging"
	"kafka-golang-analytics/internal/storage"
	"log/slog"
	"reflect"
	"strings"
	"testing"
)

var configurationVariables = []string{
	"KAFKA_BROKERS",
	"LOG_LEVEL",
	"POSTGRES_DSN",
	"POSTGRES_CONNECTION_TIMEOUT",
	"POSTGRES_MAX_OPEN_CONNS",
	"PRODUCER_TOPIC",
	"PRODUCER_HTTP_ADDR",
	"PRODUCER_BUFFER_DRAINING_TIMEOUT",
	"HTTP_ADDR",
	"HTTP_READ_TIMEOUT",
	"HTTP_SHUTDOWN_TIMEOUT",
	"API_QUERY_TIMEOUT",
	"CONSUMER_TOPIC",
	"KAFKA_GROUP_ID",
	"CONSUMER_DRAINING_TIMEOUT",
	"CONSUMER_FLUSH_TIMEOUT",
	"CONSUMER_PARTITION_BUFFER",
	"CONSUMER_MAX_POLL_RECORDS",
}

func withNoConfigurationInTheEnvironment(t *testing.T) {
	t.Helper()
	for _, variable := range configurationVariables {
		t.Setenv(variable, "")
	}
}

func withWarningsCaptured(t *testing.T) *bytes.Buffer {
	t.Helper()
	var captured bytes.Buffer
	original := warnings
	warnings = slog.New(slog.NewJSONHandler(&captured, nil))
	t.Cleanup(func() { warnings = original })
	return &captured
}

func withEnvironment(t *testing.T, variables map[string]string) {
	t.Helper()
	withNoConfigurationInTheEnvironment(t)
	for name, value := range variables {
		t.Setenv(name, value)
	}
}

func TestEnvOrDefaultReturnsTheEnvironmentValueWhenItIsSet(t *testing.T) {
	t.Setenv("CONFIG_TEST_VALUE", "from-environment")

	if got := envOrDefault("CONFIG_TEST_VALUE", "fallback"); got != "from-environment" {
		t.Fatalf("envOrDefault = %q, want from-environment", got)
	}
}

func TestEnvOrDefaultReturnsTheFallbackWhenTheVariableIsEmpty(t *testing.T) {
	t.Setenv("CONFIG_TEST_VALUE", "")

	if got := envOrDefault("CONFIG_TEST_VALUE", "fallback"); got != "fallback" {
		t.Fatalf("envOrDefault = %q, want fallback", got)
	}
}

func TestEnvAsIntParsesAWholeNumber(t *testing.T) {
	t.Setenv("CONFIG_TEST_NUMBER", "42")

	if got := envAsInt("CONFIG_TEST_NUMBER", 7); got != 42 {
		t.Fatalf("envAsInt = %d, want 42", got)
	}
}

func TestEnvAsIntAcceptsZeroAsAnExplicitValue(t *testing.T) {
	t.Setenv("CONFIG_TEST_NUMBER", "0")

	if got := envAsInt("CONFIG_TEST_NUMBER", 7); got != 0 {
		t.Fatalf("envAsInt = %d, want 0 rather than the default", got)
	}
}

func TestEnvAsIntReturnsTheFallbackWhenTheVariableIsEmpty(t *testing.T) {
	t.Setenv("CONFIG_TEST_NUMBER", "")

	if got := envAsInt("CONFIG_TEST_NUMBER", 7); got != 7 {
		t.Fatalf("envAsInt = %d, want 7", got)
	}
}

func TestEnvAsIntReturnsTheFallbackWhenTheValueIsNotANumber(t *testing.T) {
	withWarningsCaptured(t)
	cases := map[string]string{
		"letters":              "ten",
		"a decimal":            "1.5",
		"a number with a unit": "10s",
	}

	for name, value := range cases {
		t.Run(name, func(t *testing.T) {
			t.Setenv("CONFIG_TEST_NUMBER", value)

			if got := envAsInt("CONFIG_TEST_NUMBER", 7); got != 7 {
				t.Fatalf("envAsInt(%q) = %d, want the fallback 7", value, got)
			}
		})
	}
}

func TestLoadBaseConfigSplitsTheBrokerListOnCommas(t *testing.T) {
	withEnvironment(t, map[string]string{"KAFKA_BROKERS": "broker-1:19092,broker-2:19092,broker-3:19092"})

	got := LoadBaseConfig().Brokers

	want := []string{"broker-1:19092", "broker-2:19092", "broker-3:19092"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Brokers = %v, want %v", got, want)
	}
}

func TestLoadBaseConfigKeepsASingleBrokerAsOneEntry(t *testing.T) {
	withEnvironment(t, map[string]string{"KAFKA_BROKERS": "localhost:9092"})

	got := LoadBaseConfig().Brokers

	if !reflect.DeepEqual(got, []string{"localhost:9092"}) {
		t.Fatalf("Brokers = %v, want [localhost:9092]", got)
	}
}

func TestLoadBaseConfigLogsAtInfoLevelByDefault(t *testing.T) {
	withNoConfigurationInTheEnvironment(t)

	if got := LoadBaseConfig().Logging.Level; got != "info" {
		t.Fatalf("Logging.Level = %q, want info", got)
	}
}

func TestLoadBaseConfigTakesTheLogLevelFromTheEnvironment(t *testing.T) {
	withEnvironment(t, map[string]string{"LOG_LEVEL": "debug"})

	if got := LoadBaseConfig().Logging.Level; got != "debug" {
		t.Fatalf("Logging.Level = %q, want debug", got)
	}
}

func TestLoadDatabaseConfigUsesTheLocalDevelopmentDatabaseByDefault(t *testing.T) {
	withNoConfigurationInTheEnvironment(t)

	got := LoadDatabaseConfig()

	want := storage.DatabaseConfig{
		Dsn:               "postgres://analytics_chall@localhost:5432/analytics_db",
		ConnectionTimeout: 30,
		MaxOpenConns:      10,
	}
	if got != want {
		t.Fatalf("LoadDatabaseConfig = %+v, want %+v", got, want)
	}
}

func TestLoadDatabaseConfigTakesEveryValueFromTheEnvironment(t *testing.T) {
	withEnvironment(t, map[string]string{
		"POSTGRES_DSN":                "postgres://user@db:5432/stats",
		"POSTGRES_CONNECTION_TIMEOUT": "5",
		"POSTGRES_MAX_OPEN_CONNS":     "25",
	})

	got := LoadDatabaseConfig()

	want := storage.DatabaseConfig{Dsn: "postgres://user@db:5432/stats", ConnectionTimeout: 5, MaxOpenConns: 25}
	if got != want {
		t.Fatalf("LoadDatabaseConfig = %+v, want %+v", got, want)
	}
}

func TestLoadDatabaseConfigFallsBackToDefaultTimeoutsWhenTheyAreNotNumbers(t *testing.T) {
	withWarningsCaptured(t)
	withEnvironment(t, map[string]string{
		"POSTGRES_CONNECTION_TIMEOUT": "soon",
		"POSTGRES_MAX_OPEN_CONNS":     "many",
	})

	got := LoadDatabaseConfig()

	if got.ConnectionTimeout != 30 || got.MaxOpenConns != 10 {
		t.Fatalf("LoadDatabaseConfig = %+v, want timeout 30 and max connections 10", got)
	}
}

func TestLoadOrchestratorConfigUsesSafeDefaults(t *testing.T) {
	withNoConfigurationInTheEnvironment(t)

	got := LoadOrchestratorConfig()

	want := consumerorchestrator.ConsumerOrchestratorConfig{
		ConsumerFlushTimeout: 10,
		DrainingTimeout:      10,
		PartitionBuffer:      2,
		MaxPollRecords:       500,
	}
	if got != want {
		t.Fatalf("LoadOrchestratorConfig = %+v, want %+v", got, want)
	}
}

func TestLoadOrchestratorConfigTakesEveryValueFromTheEnvironment(t *testing.T) {
	withEnvironment(t, map[string]string{
		"CONSUMER_FLUSH_TIMEOUT":    "3",
		"CONSUMER_DRAINING_TIMEOUT": "4",
		"CONSUMER_PARTITION_BUFFER": "5",
		"CONSUMER_MAX_POLL_RECORDS": "6",
	})

	got := LoadOrchestratorConfig()

	want := consumerorchestrator.ConsumerOrchestratorConfig{
		ConsumerFlushTimeout: 3,
		DrainingTimeout:      4,
		PartitionBuffer:      5,
		MaxPollRecords:       6,
	}
	if got != want {
		t.Fatalf("LoadOrchestratorConfig = %+v, want %+v", got, want)
	}
}

func TestLoadProducerConfigPublishesToTheUserActivityTopicByDefault(t *testing.T) {
	withNoConfigurationInTheEnvironment(t)

	got := LoadProducerConfig()

	if got.ProduceTopic != "incoming.user_activity" {
		t.Fatalf("ProduceTopic = %q, want incoming.user_activity", got.ProduceTopic)
	}
}

func TestLoadProducerConfigUsesDefaultHTTPAndDrainingTimeouts(t *testing.T) {
	withNoConfigurationInTheEnvironment(t)

	got := LoadProducerConfig()

	if got.HttpReadTimeout != 3 || got.HttpShutdownTimeout != 10 || got.BufferDrainingTimeout != 60 {
		t.Fatalf("timeouts = read %d, shutdown %d, draining %d; want 3, 10, 60",
			got.HttpReadTimeout, got.HttpShutdownTimeout, got.BufferDrainingTimeout)
	}
}

func TestLoadProducerConfigTakesEveryValueFromTheEnvironment(t *testing.T) {
	withEnvironment(t, map[string]string{
		"KAFKA_BROKERS":                    "broker-1:19092,broker-2:19092",
		"LOG_LEVEL":                        "warn",
		"PRODUCER_TOPIC":                   "custom.topic",
		"PRODUCER_HTTP_ADDR":               "127.0.0.1:9001",
		"HTTP_READ_TIMEOUT":                "7",
		"HTTP_SHUTDOWN_TIMEOUT":            "8",
		"PRODUCER_BUFFER_DRAINING_TIMEOUT": "9",
	})

	got := LoadProducerConfig()

	want := ProducerConfig{
		BaseConfig: BaseConfig{
			Brokers: []string{"broker-1:19092", "broker-2:19092"},
			Logging: logging.LoggingConfig{Level: "warn"},
		},
		ProduceTopic:          "custom.topic",
		HttpAddr:              "127.0.0.1:9001",
		HttpReadTimeout:       7,
		HttpShutdownTimeout:   8,
		BufferDrainingTimeout: 9,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("LoadProducerConfig = %+v, want %+v", got, want)
	}
}

func TestLoadAPIConfigListensOnPort8080ByDefault(t *testing.T) {
	withNoConfigurationInTheEnvironment(t)

	if got := LoadAPIConfig().HttpAddr; got != ":8080" {
		t.Fatalf("HttpAddr = %q, want :8080", got)
	}
}

func TestLoadAPIConfigUsesDefaultTimeoutsAndLogLevel(t *testing.T) {
	withNoConfigurationInTheEnvironment(t)

	got := LoadAPIConfig()

	if got.HttpReadTimeout != 3 || got.HttpShutdownTimeout != 10 || got.QueryTimeout != 5 {
		t.Fatalf("timeouts = read %d, shutdown %d, query %d; want 3, 10, 5",
			got.HttpReadTimeout, got.HttpShutdownTimeout, got.QueryTimeout)
	}
	if got.Logging.Level != "info" {
		t.Fatalf("Logging.Level = %q, want info", got.Logging.Level)
	}
}

func TestLoadAPIConfigSharesTheDatabaseSettingsWithTheConsumer(t *testing.T) {
	withEnvironment(t, map[string]string{"POSTGRES_DSN": "postgres://user@db:5432/stats"})

	if got := LoadAPIConfig().DatabaseConfig; got != LoadDatabaseConfig() {
		t.Fatalf("API database config = %+v, want %+v", got, LoadDatabaseConfig())
	}
}

func TestLoadAPIConfigTakesEveryValueFromTheEnvironment(t *testing.T) {
	withEnvironment(t, map[string]string{
		"LOG_LEVEL":             "error",
		"HTTP_ADDR":             "127.0.0.1:9000",
		"HTTP_READ_TIMEOUT":     "11",
		"HTTP_SHUTDOWN_TIMEOUT": "12",
		"API_QUERY_TIMEOUT":     "13",
		"POSTGRES_DSN":          "postgres://user@db:5432/stats",
	})

	got := LoadAPIConfig()

	want := APIConfig{
		Logging:             logging.LoggingConfig{Level: "error"},
		DatabaseConfig:      LoadDatabaseConfig(),
		HttpAddr:            "127.0.0.1:9000",
		HttpReadTimeout:     11,
		HttpShutdownTimeout: 12,
		QueryTimeout:        13,
	}
	if got != want {
		t.Fatalf("LoadAPIConfig = %+v, want %+v", got, want)
	}
}

func TestLoadConsumerConfigReadsTheUserActivityTopicInTheAnalyticsGroupByDefault(t *testing.T) {
	withNoConfigurationInTheEnvironment(t)

	got := LoadConsumerConfig()

	if got.ConsumerTopic != "incoming.user_activity" || got.GroupID != "analytics-agg" {
		t.Fatalf("topic %q, group %q; want incoming.user_activity and analytics-agg", got.ConsumerTopic, got.GroupID)
	}
}

func TestLoadConsumerConfigDrainsForTenSecondsByDefault(t *testing.T) {
	withNoConfigurationInTheEnvironment(t)

	if got := LoadConsumerConfig().ConsumerDrainingTimeout; got != 10 {
		t.Fatalf("ConsumerDrainingTimeout = %d, want 10", got)
	}
}

func TestLoadConsumerConfigIncludesTheOrchestratorAndDatabaseSettings(t *testing.T) {
	withEnvironment(t, map[string]string{
		"CONSUMER_PARTITION_BUFFER": "8",
		"POSTGRES_MAX_OPEN_CONNS":   "20",
	})

	got := LoadConsumerConfig()

	if got.ConsumerOrchestrator.PartitionBuffer != 8 {
		t.Fatalf("ConsumerOrchestrator.PartitionBuffer = %d, want 8", got.ConsumerOrchestrator.PartitionBuffer)
	}
	if got.DatabaseConfig.MaxOpenConns != 20 {
		t.Fatalf("DatabaseConfig.MaxOpenConns = %d, want 20", got.DatabaseConfig.MaxOpenConns)
	}
}

func TestLoadConsumerConfigTakesTopicGroupAndBrokersFromTheEnvironment(t *testing.T) {
	withEnvironment(t, map[string]string{
		"KAFKA_BROKERS":             "broker-1:19092,broker-2:19092",
		"CONSUMER_TOPIC":            "custom.topic",
		"KAFKA_GROUP_ID":            "custom-group",
		"CONSUMER_DRAINING_TIMEOUT": "15",
	})

	got := LoadConsumerConfig()

	if got.ConsumerTopic != "custom.topic" || got.GroupID != "custom-group" || got.ConsumerDrainingTimeout != 15 {
		t.Fatalf("topic %q, group %q, draining %d; want custom.topic, custom-group, 15",
			got.ConsumerTopic, got.GroupID, got.ConsumerDrainingTimeout)
	}
	if !reflect.DeepEqual(got.Brokers, []string{"broker-1:19092", "broker-2:19092"}) {
		t.Fatalf("Brokers = %v", got.Brokers)
	}
}

func TestEnvOrDefaultTrimsSurroundingWhitespaceFromTheValue(t *testing.T) {
	t.Setenv("CONFIG_TEST_VALUE", "  from-environment \t")

	if got := envOrDefault("CONFIG_TEST_VALUE", "fallback"); got != "from-environment" {
		t.Fatalf("envOrDefault = %q, want from-environment", got)
	}
}

func TestEnvOrDefaultTreatsAWhitespaceOnlyValueAsUnset(t *testing.T) {
	t.Setenv("CONFIG_TEST_VALUE", "   \t ")

	if got := envOrDefault("CONFIG_TEST_VALUE", "fallback"); got != "fallback" {
		t.Fatalf("envOrDefault = %q, want fallback", got)
	}
}

func TestEnvAsIntIgnoresSurroundingWhitespace(t *testing.T) {
	warned := withWarningsCaptured(t)
	t.Setenv("CONFIG_TEST_NUMBER", " 10 ")

	if got := envAsInt("CONFIG_TEST_NUMBER", 7); got != 10 {
		t.Fatalf("envAsInt = %d, want 10", got)
	}
	if warned.Len() != 0 {
		t.Fatalf("a padded number should not warn, got %s", warned)
	}
}

func TestEnvAsIntWarnsNamingTheVariableAndTheValueItIgnored(t *testing.T) {
	warned := withWarningsCaptured(t)
	t.Setenv("CONFIG_TEST_NUMBER", "ten")

	envAsInt("CONFIG_TEST_NUMBER", 7)

	for _, expected := range []string{`"level":"WARN"`, "CONFIG_TEST_NUMBER", `"value":"ten"`, `"default":7`} {
		if !strings.Contains(warned.String(), expected) {
			t.Errorf("warning %q does not mention %s", warned, expected)
		}
	}
}

func TestEnvAsIntWarnsOnceForEachInvalidVariable(t *testing.T) {
	warned := withWarningsCaptured(t)
	t.Setenv("CONFIG_TEST_NUMBER", "ten")

	envAsInt("CONFIG_TEST_NUMBER", 7)
	envAsInt("CONFIG_TEST_NUMBER", 7)

	if got := strings.Count(warned.String(), "\n"); got != 2 {
		t.Fatalf("got %d warnings for 2 reads of an invalid variable, want 2", got)
	}
}

func TestEnvAsIntStaysQuietWhenTheVariableIsUnsetOrBlank(t *testing.T) {
	for name, value := range map[string]string{"unset": "", "only whitespace": "   "} {
		t.Run(name, func(t *testing.T) {
			warned := withWarningsCaptured(t)
			t.Setenv("CONFIG_TEST_NUMBER", value)

			if got := envAsInt("CONFIG_TEST_NUMBER", 7); got != 7 {
				t.Fatalf("envAsInt = %d, want the default 7", got)
			}
			if warned.Len() != 0 {
				t.Fatalf("expected no warning, got %s", warned)
			}
		})
	}
}

func TestEnvAsIntStaysQuietWhenTheValueIsValid(t *testing.T) {
	warned := withWarningsCaptured(t)
	t.Setenv("CONFIG_TEST_NUMBER", "42")

	envAsInt("CONFIG_TEST_NUMBER", 7)

	if warned.Len() != 0 {
		t.Fatalf("expected no warning, got %s", warned)
	}
}

func TestLoadDatabaseConfigWarnsAboutEveryNumberItCouldNotRead(t *testing.T) {
	warned := withWarningsCaptured(t)
	withEnvironment(t, map[string]string{
		"POSTGRES_CONNECTION_TIMEOUT": "soon",
		"POSTGRES_MAX_OPEN_CONNS":     "many",
	})

	LoadDatabaseConfig()

	for _, variable := range []string{"POSTGRES_CONNECTION_TIMEOUT", "POSTGRES_MAX_OPEN_CONNS"} {
		if !strings.Contains(warned.String(), variable) {
			t.Errorf("no warning about %s in %s", variable, warned)
		}
	}
}

func TestLoadBaseConfigTrimsSpacesAroundEachBroker(t *testing.T) {
	withEnvironment(t, map[string]string{"KAFKA_BROKERS": " broker-1:19092 ,\tbroker-2:19092 "})

	got := LoadBaseConfig().Brokers

	if !reflect.DeepEqual(got, []string{"broker-1:19092", "broker-2:19092"}) {
		t.Fatalf("Brokers = %q", got)
	}
}

func TestLoadBaseConfigDropsEmptyBrokerEntries(t *testing.T) {
	withEnvironment(t, map[string]string{"KAFKA_BROKERS": ",broker-1:19092,, ,broker-2:19092,"})

	got := LoadBaseConfig().Brokers

	if !reflect.DeepEqual(got, []string{"broker-1:19092", "broker-2:19092"}) {
		t.Fatalf("Brokers = %q", got)
	}
}

func TestLoadBaseConfigHasNoBrokersWhenTheVariableIsUnsetEmptyOrOnlySeparators(t *testing.T) {
	for name, value := range map[string]string{"unset": "", "only whitespace": "  ", "only commas": " , ,,"} {
		t.Run(name, func(t *testing.T) {
			withEnvironment(t, map[string]string{"KAFKA_BROKERS": value})

			if got := LoadBaseConfig().Brokers; len(got) != 0 {
				t.Fatalf("Brokers = %q, want none", got)
			}
		})
	}
}

func TestBaseConfigWithBrokersIsValid(t *testing.T) {
	config := BaseConfig{Brokers: []string{"broker-1:19092"}}

	if err := config.Validate(); err != nil {
		t.Fatalf("Validate returned %v, want nil", err)
	}
}

func TestBaseConfigWithoutBrokersIsInvalidAndNamesTheVariableToSet(t *testing.T) {
	err := BaseConfig{}.Validate()

	if err == nil || !strings.Contains(err.Error(), "KAFKA_BROKERS") {
		t.Fatalf("Validate returned %v, want an error naming KAFKA_BROKERS", err)
	}
}

func TestProducerConfigWithoutBrokersInTheEnvironmentIsInvalid(t *testing.T) {
	withNoConfigurationInTheEnvironment(t)

	if err := LoadProducerConfig().Validate(); err == nil {
		t.Fatal("expected the producer configuration to be invalid without KAFKA_BROKERS")
	}
}

func TestConsumerConfigWithoutBrokersInTheEnvironmentIsInvalid(t *testing.T) {
	withNoConfigurationInTheEnvironment(t)

	if err := LoadConsumerConfig().Validate(); err == nil {
		t.Fatal("expected the consumer configuration to be invalid without KAFKA_BROKERS")
	}
}

func TestProducerAndConsumerConfigsAreValidOnceBrokersAreSet(t *testing.T) {
	withEnvironment(t, map[string]string{"KAFKA_BROKERS": "localhost:9092"})

	if err := LoadProducerConfig().Validate(); err != nil {
		t.Errorf("producer configuration invalid: %v", err)
	}
	if err := LoadConsumerConfig().Validate(); err != nil {
		t.Errorf("consumer configuration invalid: %v", err)
	}
}

func TestLoadProducerConfigServesOnPort8081ByDefault(t *testing.T) {
	withNoConfigurationInTheEnvironment(t)

	if got := LoadProducerConfig().HttpAddr; got != ":8081" {
		t.Fatalf("HttpAddr = %q, want :8081", got)
	}
}

func TestLoadProducerConfigTakesItsAddressFromProducerHTTPAddr(t *testing.T) {
	withEnvironment(t, map[string]string{"PRODUCER_HTTP_ADDR": "127.0.0.1:9001", "HTTP_ADDR": "127.0.0.1:9002"})

	if got := LoadProducerConfig().HttpAddr; got != "127.0.0.1:9001" {
		t.Fatalf("HttpAddr = %q, want the PRODUCER_HTTP_ADDR value", got)
	}
}

func TestProducerAndAPIDefaultToDifferentPortsSoTheyCanRunOnTheSameHost(t *testing.T) {
	withNoConfigurationInTheEnvironment(t)

	if LoadProducerConfig().HttpAddr == LoadAPIConfig().HttpAddr {
		t.Fatalf("producer and api both default to %q", LoadAPIConfig().HttpAddr)
	}
}
