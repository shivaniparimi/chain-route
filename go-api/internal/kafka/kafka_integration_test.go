//go:build kafka_integration

package kafka

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

var testBrokers = []string{"localhost:9092"}

// publishWithRetry retries the first publish to a freshly created topic,
// because kafka-go's Writer caches topic/partition metadata with a
// randomized refresh interval (see kafka-go's transport.go) and may not
// yet know about a topic that was created moments ago, even though the
// broker already has it. This is a test-only concern: production's
// Producer publishes to a long-lived, already-provisioned topic and never
// hits this race.
func publishWithRetry(t *testing.T, ctx context.Context, producer *Producer, key string, value []byte) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		if err := producer.Publish(ctx, key, value); err == nil {
			return
		} else {
			lastErr = err
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("publish did not succeed within the retry window: %v", lastErr)
}

func TestProducerConsumer_RoundTrip(t *testing.T) {
	topic := fmt.Sprintf("chainroute.payments.routed.test-roundtrip-%d", time.Now().UnixNano())

	conn, err := kafkago.Dial("tcp", testBrokers[0])
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	if err := conn.CreateTopics(kafkago.TopicConfig{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	time.Sleep(500 * time.Millisecond)

	producer := NewProducer(testBrokers, topic)
	defer producer.Close()
	consumer := NewConsumer(ConsumerConfig{Brokers: testBrokers, Topic: topic, GroupID: fmt.Sprintf("test-roundtrip-%d", time.Now().UnixNano())})
	defer consumer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	publishWithRetry(t, ctx, producer, "payment-123", []byte(`{"payment_id":"payment-123"}`))

	msg, err := consumer.FetchMessage(ctx)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if string(msg.Key) != "payment-123" {
		t.Fatalf("expected key payment-123, got %q", msg.Key)
	}
	if string(msg.Value) != `{"payment_id":"payment-123"}` {
		t.Fatalf("unexpected value: %s", msg.Value)
	}
	if err := consumer.CommitMessages(ctx, msg); err != nil {
		t.Fatalf("commit: %v", err)
	}
}

func TestConsumerGroup_SplitsMessagesAcrossPartitions(t *testing.T) {
	topic := fmt.Sprintf("chainroute.payments.routed.test-group-%d", time.Now().UnixNano())
	group := fmt.Sprintf("test-group-%d", time.Now().UnixNano())

	conn, err := kafkago.Dial("tcp", testBrokers[0])
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	if err := conn.CreateTopics(kafkago.TopicConfig{Topic: topic, NumPartitions: 2, ReplicationFactor: 1}); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	time.Sleep(500 * time.Millisecond)

	producer := NewProducer(testBrokers, topic)
	defer producer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	const n = 20
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%d", i)
		if i == 0 {
			// First publish to the freshly created topic: retry until the
			// producer's cached metadata catches up with the broker.
			publishWithRetry(t, ctx, producer, key, []byte(key))
			continue
		}
		if err := producer.Publish(ctx, key, []byte(key)); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	consumerA := NewConsumer(ConsumerConfig{Brokers: testBrokers, Topic: topic, GroupID: group})
	defer consumerA.Close()
	consumerB := NewConsumer(ConsumerConfig{Brokers: testBrokers, Topic: topic, GroupID: group})
	defer consumerB.Close()

	var mu sync.Mutex
	seenByA, seenByB := 0, 0
	var wg sync.WaitGroup
	wg.Add(2)
	drain := func(c *Consumer, count *int) {
		defer wg.Done()
		for {
			fctx, fcancel := context.WithTimeout(ctx, 3*time.Second)
			msg, err := c.FetchMessage(fctx)
			fcancel()
			if err != nil {
				return
			}
			mu.Lock()
			*count++
			mu.Unlock()
			_ = c.CommitMessages(ctx, msg)
		}
	}
	go drain(consumerA, &seenByA)
	go drain(consumerB, &seenByB)
	wg.Wait()

	if seenByA+seenByB != n {
		t.Fatalf("expected %d total messages consumed, got %d (A=%d, B=%d)", n, seenByA+seenByB, seenByA, seenByB)
	}
	if seenByA == 0 || seenByB == 0 {
		t.Fatalf("expected both consumers to receive at least one message, got A=%d B=%d", seenByA, seenByB)
	}
}
