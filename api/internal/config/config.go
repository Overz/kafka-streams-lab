// Package config assembles the application's configuration from environment
// variables. Every service loads the same Config struct but only reads the
// sections it needs — the gateway ignores SMTP, the notifier ignores Storage,
// and so on. Centralizing this keeps env-var names defined in exactly one place.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Env      string // "development" | "production"
	LogLevel string // "debug" | "info" | "warn" | "error"

	HTTP  HTTPConfig
	Kafka KafkaConfig
}

type KafkaConfig struct {
	Brokers      []string
	WriteTimeout time.Duration
	Topics       struct {
		Order KafkaTopicSpecs
	}
}

type KafkaTopicSpecs struct {
	Topic string
}

type HTTPConfig struct {
	Port            string
	ShutdownTimeout time.Duration
}

func Load() (Config, error) {
	l := &loader{}

	cfg := Config{
		Env:      l.str("APP_ENV", "dev"),
		LogLevel: l.str("LOG_LEVEL", "debug"),

		HTTP: HTTPConfig{
			Port:            l.str("HTTP_PORT", "8090"),
			ShutdownTimeout: l.duration("HTTP_SHUTDOWN_TIMEOUT", 10*time.Second),
		},
		Kafka: KafkaConfig{
			Brokers:      l.slice("KAFKA_BROKERS", "localhost:19092,localhost:19093,localhost:19094"),
			WriteTimeout: l.duration("KAFKA_WRITE_TIMEOUT", 10*time.Second),
			Topics: struct{ Order KafkaTopicSpecs }{
				Order: KafkaTopicSpecs{
					Topic: l.str("ORDER_INPUT_TOPIC", "order.created"),
				},
			},
		},
	}

	return cfg, l.err
}

type loader struct {
	err error
}

func (l *loader) str(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// slice splits the default too, not only the environment value. Returning
// []string{def} would make a multi-address default such as the three cluster-a
// brokers collapse into one bogus entry with commas in the host name.
func (l *loader) slice(key, def string) []string {
	v := os.Getenv(key)
	if v == "" {
		v = def
	}
	return strings.Split(v, ",")
}

func (l *loader) int(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		l.record(key, err)
		return def
	}
	return n
}

func (l *loader) boolean(key string, def bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		l.record(key, err)
		return def
	}
	return b
}

func (l *loader) duration(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		l.record(key, err)
		return def
	}
	return d
}

// record keeps only the first error so the caller sees the earliest problem.
func (l *loader) record(key string, err error) {
	if l.err == nil {
		l.err = fmt.Errorf("config: invalid value for %s: %w", key, err)
	}
}
