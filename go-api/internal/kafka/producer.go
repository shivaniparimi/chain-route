package kafka

import (
	"context"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

// Producer publishes messages to a single Kafka topic, keyed by payment ID
// so that any future event type for the same payment lands on the same
// partition (no ordering requirement exists today -- see the Phase 6
// design spec §14 -- this costs nothing and avoids a harder migration
// later).
type Producer struct {
	writer *kafkago.Writer
}

func NewProducer(brokers []string, topic string) *Producer {
	return &Producer{
		writer: &kafkago.Writer{
			Addr:         kafkago.TCP(brokers...),
			Topic:        topic,
			Balancer:     &kafkago.Hash{},
			RequiredAcks: kafkago.RequireOne,
			// kafka-go's Writer defaults to a 1s BatchTimeout, which is
			// tuned for high-throughput batched producers -- payment
			// events are latency-sensitive and typically published one
			// at a time, so a low timeout keeps Publish from blocking on
			// an idle batch timer for up to a second per call.
			BatchTimeout: 10 * time.Millisecond,
			// The target topic is expected to be provisioned ahead of
			// time (see the Phase 6 design spec / Task 1's topic
			// creation, which creates chainroute.payments.routed with 3
			// partitions). Auto-creation is deliberately left disabled
			// (kafka-go's default): if the topic is ever missing, we want
			// a loud "unknown topic" error rather than kafka-go silently
			// auto-creating it with the wrong partition count.
		},
	}
}

// Publish sends value to the topic with the given key.
func (p *Producer) Publish(ctx context.Context, key string, value []byte) error {
	return p.writer.WriteMessages(ctx, kafkago.Message{
		Key:   []byte(key),
		Value: value,
	})
}

func (p *Producer) Close() error {
	return p.writer.Close()
}
