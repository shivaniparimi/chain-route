package kafka

import (
	"context"

	kafkago "github.com/segmentio/kafka-go"
)

type ConsumerConfig struct {
	Brokers []string
	Topic   string
	GroupID string
}

// Consumer reads messages from a topic within a consumer group. Offsets
// are committed ONLY via an explicit call to CommitMessages -- the reader's
// CommitInterval is deliberately left at its zero value, which makes
// kafka-go commit synchronously on CommitMessages rather than on a
// background timer (see the Phase 6 design spec §13: manual commit,
// issued only after a message reaches a definitive outcome).
type Consumer struct {
	reader *kafkago.Reader
}

func NewConsumer(cfg ConsumerConfig) *Consumer {
	return &Consumer{
		reader: kafkago.NewReader(kafkago.ReaderConfig{
			Brokers: cfg.Brokers,
			Topic:   cfg.Topic,
			GroupID: cfg.GroupID,
		}),
	}
}

// FetchMessage blocks until a message is available or ctx is done. It does
// NOT commit the offset -- call CommitMessages only after the message has
// been fully, durably handled.
func (c *Consumer) FetchMessage(ctx context.Context) (kafkago.Message, error) {
	return c.reader.FetchMessage(ctx)
}

// CommitMessages commits the offsets for the given messages.
func (c *Consumer) CommitMessages(ctx context.Context, msgs ...kafkago.Message) error {
	return c.reader.CommitMessages(ctx, msgs...)
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}
