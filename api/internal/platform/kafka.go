// Package platform holds the infrastructure adapters shared by the API's
// handlers. A single KafkaPublisher fronts one kafka.Writer for the whole
// process: connections, batching and compression are pooled once, while the
// destination topic is decided per message by the event being published.
package platform

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/compress"
)

// Topic is the logical name of a stream. Handlers reference these constants and
// never the real topic name — that one comes from configuration through the
// registry given to NewKafkaPublisher, so renaming a topic stays a config change.
type Topic string

const (
	TopicOrderCreated Topic = "order.created"
)

// Event is a payload that knows where it belongs. Implementing it is what lets
// an endpoint call Publish without naming (or mistyping) a topic.
type Event interface {
	Topic() Topic
	Key() *string
}

const defaultWriteTimeout = 10 * time.Second

type KafkaPublisher struct {
	writer       *kafka.Writer
	topics       map[Topic]string
	writeTimeout time.Duration
}

type KafkaConfig struct {
	Brokers      []string
	Topics       map[Topic]string
	WriteTimeout time.Duration
}

func KafkaConnect(ctx context.Context, brokers []string) (err error) {
	for _, broker := range brokers {
		conn, e := kafka.DialContext(ctx, "tcp", broker)
		if e != nil {
			err = e
			continue
		}

		_ = conn.Close()

		return nil
	}

	return fmt.Errorf("failed to connect to Kafka brokers: %w", err)
}

func NewKafkaPublisher(config KafkaConfig) (*KafkaPublisher, error) {
	if len(config.Brokers) == 0 {
		return nil, fmt.Errorf("kafka publisher: no brokers configured")
	}

	if len(config.Topics) == 0 {
		return nil, fmt.Errorf("kafka publisher: no topics registered")
	}

	// Copy the registry so a caller mutating its map later cannot change where
	// already-running handlers publish.
	topics := make(map[Topic]string, len(config.Topics))
	for topic, name := range config.Topics {
		if name == "" {
			return nil, fmt.Errorf("kafka publisher: topic %q has an empty name", topic)
		}
		topics[topic] = name
	}

	writeTimeout := config.WriteTimeout
	if writeTimeout <= 0 {
		writeTimeout = defaultWriteTimeout
	}

	// Writer.Topic is left unset on purpose: kafka-go then requires every
	// message to carry its own topic, which is exactly the per-event routing
	// Publish does below.
	writer := &kafka.Writer{
		Addr: kafka.TCP(config.Brokers...),
		// Murmur2, not RoundRobin. RoundRobin ignores Message.Key entirely, so
		// two events of the same order would land on different partitions and
		// the ordering Event.Key promises would not exist. Murmur2 is also what
		// the Java producer inside Kafka Streams uses, so a key published here
		// reaches the same partition it would have reached from the JVM side.
		Balancer:     &kafka.Murmur2Balancer{},
		RequiredAcks: kafka.RequireAll,
		// Synchronous: WriteMessages only returns after the brokers acked. With
		// Async the call returned immediately, Publish could not fail, and the
		// 202 the endpoint sends back was a guess -- a broker rejecting the
		// write only showed up in this callback, long after the client left.
		Async: false,
		// The lab runs both clusters with auto.create.topics.enable=false on
		// purpose, and the config-server provisions every topic explicitly. A
		// typo in ORDER_INPUT_TOPIC has to fail loudly here, not quietly mint a
		// topic with the broker defaults (RF=1) that nobody consumes.
		AllowAutoTopicCreation: false,
		Compression:            compress.Zstd,
		Completion: func(messages []kafka.Message, err error) {
			if err != nil {
				slog.Error("failed to send kafka messages", "error", err, "count", len(messages))
				return
			}

			for _, msg := range messages {
				slog.Debug(
					"kafka message sent",
					"topic", msg.Topic,
					"partition", msg.Partition,
					"offset", msg.Offset,
					"key", string(msg.Key),
				)
			}
		},
	}

	return &KafkaPublisher{
		writer:       writer,
		topics:       topics,
		writeTimeout: writeTimeout,
	}, nil
}

func (p *KafkaPublisher) KafkaCloseConnection() error {
	return p.writer.Close()
}

// Publish routes the event to the topic it declares. The context belongs to the
// caller — an endpoint passes r.Context() so a dropped request stops the write.
func (p *KafkaPublisher) Publish(ctx context.Context, event Event) error {
	topic := event.Topic()

	name, ok := p.topics[topic]
	if !ok {
		return fmt.Errorf("publish kafka message: topic %q is not registered", topic)
	}

	value, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal kafka message: %w", err)
	}

	msg := kafka.Message{
		Topic: name,
		Value: value,
	}

	if key := event.Key(); key != nil {
		msg.Key = []byte(*key)
	}

	ctx, cancel := context.WithTimeout(ctx, p.writeTimeout)
	defer cancel()

	if err = p.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("publish kafka message to %s: %w", name, err)
	}

	return nil
}
