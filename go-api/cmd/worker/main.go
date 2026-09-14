package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"chainroute/go-api/internal/events"
	"chainroute/go-api/internal/kafka"
	"chainroute/go-api/internal/postgres"
	"chainroute/go-api/internal/worker"
)

func main() {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		log.Fatal("DATABASE_URL environment variable is required")
	}
	bootstrapServers := os.Getenv("KAFKA_BOOTSTRAP_SERVERS")
	if bootstrapServers == "" {
		log.Fatal("KAFKA_BOOTSTRAP_SERVERS environment variable is required")
	}
	brokers := strings.Split(bootstrapServers, ",")

	topic := envOrDefault("KAFKA_TOPIC", "chainroute.payments.routed")
	consumerGroup := envOrDefault("KAFKA_CONSUMER_GROUP", "chainroute-payment-worker")
	outboxPollInterval := envDuration("OUTBOX_POLL_INTERVAL_MS", 500*time.Millisecond, time.Millisecond)
	recoverySweepInterval := envDuration("WORKER_RECOVERY_SWEEP_INTERVAL_SECONDS", 30*time.Second, time.Second)
	recoveryStaleness := envDuration("WORKER_RECOVERY_STALENESS_SECONDS", 120*time.Second, time.Second)

	// PostgreSQL is blocking-ping-or-die at startup, matching cmd/server:
	// nothing in this binary can do anything useful without the database.
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		log.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(10)

	pingCtx, pingCancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := db.PingContext(pingCtx); err != nil {
		pingCancel()
		log.Fatalf("failed to connect to database: %v", err)
	}
	pingCancel()

	store := postgres.New(db)

	// Kafka is deliberately NOT blocking-or-die here: a transiently
	// unreachable broker at startup is tolerated, since kafka-go's writer
	// and reader dial lazily and retry internally rather than failing
	// fast. The recovery sweep goroutine below depends only on Postgres
	// and keeps working regardless of Kafka's availability.
	producer := kafka.NewProducer(brokers, topic)
	defer producer.Close()
	consumer := kafka.NewConsumer(kafka.ConsumerConfig{Brokers: brokers, Topic: topic, GroupID: consumerGroup})
	defer consumer.Close()

	processor := &worker.Processor{Store: store}
	publisher := &worker.Publisher{
		Store: store,
		Publish: func(ctx context.Context, key string, value []byte) error {
			return producer.Publish(ctx, key, value)
		},
	}
	recovery := &worker.Recovery{Store: store, Staleness: recoveryStaleness}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var wg sync.WaitGroup
	wg.Add(3)
	go func() { defer wg.Done(); publisher.Run(ctx, outboxPollInterval) }()
	go func() { defer wg.Done(); recovery.Run(ctx, recoverySweepInterval) }()
	go func() { defer wg.Done(); runConsumeLoop(ctx, consumer, processor) }()

	log.Printf("worker started: topic=%s group=%s brokers=%v", topic, consumerGroup, brokers)
	<-ctx.Done()
	log.Println("shutting down...")
	wg.Wait()
	log.Println("worker shut down")
}

// runConsumeLoop stops requesting new messages once ctx is done (FetchMessage
// returns an error on a cancelled context), but each in-flight message is
// handled with its own bounded context independent of the shutdown signal,
// so a message already in progress gets a grace period to finish rather
// than being aborted mid-cycle.
func runConsumeLoop(ctx context.Context, consumer *kafka.Consumer, processor *worker.Processor) {
	for {
		msg, err := consumer.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("WARNING: fetch message failed, retrying: %v", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}

		var evt events.RoutedPayment
		if err := json.Unmarshal(msg.Value, &evt); err != nil {
			log.Printf("ERROR: failed to decode event, committing offset to skip it: %v", err)
			commitCtx, commitCancel := context.WithTimeout(context.Background(), 5*time.Second)
			err := consumer.CommitMessages(commitCtx, msg)
			commitCancel()
			if err != nil {
				log.Printf("ERROR: failed to commit offset for undecodable message at key %q: %v", msg.Key, err)
			}
			continue
		}

		handleCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err = processor.HandleRoutedPayment(handleCtx, evt)
		cancel()
		if err != nil {
			log.Printf("ERROR: failed to handle routed payment %s, NOT committing offset: %v", evt.PaymentID, err)
			continue
		}

		commitCtx, commitCancel := context.WithTimeout(context.Background(), 5*time.Second)
		err = consumer.CommitMessages(commitCtx, msg)
		commitCancel()
		if err != nil {
			log.Printf("ERROR: failed to commit offset for payment %s: %v", evt.PaymentID, err)
		}
	}
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envDuration(key string, def time.Duration, unit time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		log.Fatalf("invalid %s: %v", key, err)
	}
	return time.Duration(n) * unit
}
