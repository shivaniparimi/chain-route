package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"math/big"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/ethereum/go-ethereum/common"
	_ "github.com/jackc/pgx/v5/stdlib"

	"chainroute/go-api/internal/bridge/across"
	"chainroute/go-api/internal/bridge/quote"
	"chainroute/go-api/internal/events"
	"chainroute/go-api/internal/evm"
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

	blockchainEnv := os.Getenv("BLOCKCHAIN_ENV")

	reconcileStaleness := envDuration("RECONCILE_STALENESS_SECONDS", 120*time.Second, time.Second)
	reconcileSweepInterval := envDuration("RECONCILE_SWEEP_INTERVAL_SECONDS", 30*time.Second, time.Second)
	nonceDivergenceCheckInterval := envDuration("NONCE_DIVERGENCE_CHECK_INTERVAL_SECONDS", 60*time.Second, time.Second)

	// Testnet-mode message handling gets its own, larger timeout than the
	// simulated path's fixed 10-second handleCtx below: real testnet
	// execution makes several sequential network round trips (an Across
	// quote alone can take up to across.Client's own 15-second HTTP
	// timeout), so 10 seconds is not comfortably larger than that (review
	// Finding 4). Simulated-mode handling is unaffected -- it keeps using
	// handleCtx's fixed 10 seconds unchanged.
	testnetHandleTimeout := envDuration("TESTNET_HANDLE_TIMEOUT_SECONDS", 30*time.Second, time.Second)

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

	var executor *worker.Executor
	var reconciler *worker.Reconciler

	if blockchainEnv == "testnet" {
		testnetWalletKey := os.Getenv("TESTNET_WALLET_PRIVATE_KEY")
		if testnetWalletKey == "" {
			log.Fatal("TESTNET_WALLET_PRIVATE_KEY is required when BLOCKCHAIN_ENV=testnet")
		}
		sepoliaRPC := os.Getenv("ETHEREUM_SEPOLIA_RPC_URL")
		baseSepoliaRPC := os.Getenv("BASE_SEPOLIA_RPC_URL")
		if sepoliaRPC == "" || baseSepoliaRPC == "" {
			log.Fatal("ETHEREUM_SEPOLIA_RPC_URL and BASE_SEPOLIA_RPC_URL are required when BLOCKCHAIN_ENV=testnet")
		}
		acrossBaseURL := envOrDefault("ACROSS_TESTNET_API_URL", "https://testnet.across.to/api")
		maxTestnetAmountWei := envBigInt("MAX_TESTNET_AMOUNT_WEI", big.NewInt(10_000_000_000_000_000)) // 0.01 WETH default ceiling

		wallet, err := evm.LoadWallet(testnetWalletKey)
		if err != nil {
			log.Fatalf("failed to load testnet wallet: %v", err)
		}
		log.Printf("testnet execution enabled: wallet address %s", wallet.Address.Hex())

		dialCtx, dialCancel := context.WithTimeout(context.Background(), 10*time.Second)
		sepoliaClient, err := evm.Dial(dialCtx, sepoliaRPC, 11155111)
		dialCancel()
		if err != nil {
			log.Fatalf("failed to dial Sepolia RPC: %v", err)
		}
		dialCtx2, dialCancel2 := context.WithTimeout(context.Background(), 10*time.Second)
		_, err = evm.Dial(dialCtx2, baseSepoliaRPC, 84532)
		dialCancel2()
		if err != nil {
			log.Fatalf("failed to dial Base Sepolia RPC: %v", err)
		}

		seedCtx, seedCancel := context.WithTimeout(context.Background(), 10*time.Second)
		pendingNonce, err := sepoliaClient.PendingNonceAt(seedCtx, wallet.Address)
		seedCancel()
		if err != nil {
			log.Fatalf("failed to query starting nonce: %v", err)
		}
		seedCtx2, seedCancel2 := context.WithTimeout(context.Background(), 5*time.Second)
		err = store.SeedWalletNonce(seedCtx2, wallet.Address.Hex(), int64(pendingNonce))
		seedCancel2()
		if err != nil {
			log.Fatalf("failed to seed wallet nonce: %v", err)
		}

		acrossClient := across.NewClient(acrossBaseURL)
		acrossClient.APIKey = os.Getenv("ACROSS_API_KEY")
		acrossClient.IntegratorID = os.Getenv("ACROSS_INTEGRATOR_ID")

		maxFeeSlippageBps := envInt64("MAX_FEE_SLIPPAGE_BPS", 500) // 5% default
		routingQuoteTTL := envDuration("ROUTING_QUOTE_TTL_SECONDS", 2*time.Minute, time.Second)
		quoteProviders := map[string]quote.Provider{
			"across": across.NewProvider(acrossClient, routingQuoteTTL),
		}

		executor = &worker.Executor{
			Store: store, Wallet: wallet, OriginClient: sepoliaClient, Across: acrossClient,
			BridgeProvider: "across", OriginChainID: 11155111, DestChainID: 84532,
			SpokePoolAddress: common.HexToAddress("0x5ef6C01E11889d86803e0B23e3cB3F9E9d97B662"),
			WETHOrigin:       common.HexToAddress("0xfFf9976782d46CC05630D1f6eBAb18b2324d6B14"),
			WETHDestination:  common.HexToAddress("0x4200000000000000000000000000000000000006"),
			MaxAmountWei:     maxTestnetAmountWei,
			QuoteProviders:   quoteProviders, MaxFeeSlippageBps: maxFeeSlippageBps,
		}
		reconciler = &worker.Reconciler{
			Store: store, Executor: executor, OriginClient: sepoliaClient, Across: acrossClient,
			WalletAddress: wallet.Address, OriginChainID: 11155111, Staleness: reconcileStaleness,
		}
	}

	// Processor.Executor is declared as the TestnetExecutor interface, and a
	// nil *worker.Executor assigned directly into an interface field would
	// produce a non-nil interface (Go's typed-nil-in-interface trap) -- that
	// would make HandleRoutedPayment's `p.Executor == nil` check evaluate
	// false even when testnet execution is disabled. Guard the assignment
	// explicitly so the interface field is a genuine nil when executor is nil.
	var processorExecutor worker.TestnetExecutor
	if executor != nil {
		processorExecutor = executor
	}
	processor := &worker.Processor{Store: store, Executor: processorExecutor, TestnetTimeout: testnetHandleTimeout}

	// Kafka is deliberately NOT blocking-or-die here: a transiently
	// unreachable broker at startup is tolerated, since kafka-go's writer
	// and reader dial lazily and retry internally rather than failing
	// fast. The recovery sweep goroutine below depends only on Postgres
	// and keeps working regardless of Kafka's availability.
	producer := kafka.NewProducer(brokers, topic)
	defer producer.Close()
	consumer := kafka.NewConsumer(kafka.ConsumerConfig{Brokers: brokers, Topic: topic, GroupID: consumerGroup})
	defer consumer.Close()

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
	goroutines := 3
	if reconciler != nil {
		goroutines = 4
	}
	wg.Add(goroutines)
	go func() { defer wg.Done(); publisher.Run(ctx, outboxPollInterval) }()
	go func() { defer wg.Done(); recovery.Run(ctx, recoverySweepInterval) }()
	go func() { defer wg.Done(); runConsumeLoop(ctx, consumer, processor) }()
	if reconciler != nil {
		go func() { defer wg.Done(); reconciler.Run(ctx, reconcileSweepInterval, nonceDivergenceCheckInterval) }()
	}

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

		// This 10-second budget is unchanged from Phase 6 and stays exactly
		// as-is for simulated-mode payments (the only kind this worker
		// handled prior to Phase 7). It also bounds ClaimPayment for
		// testnet-mode payments, which is fine -- that's a single fast DB
		// write, not the network-heavy part. HandleRoutedPayment itself
		// swaps in a separate, larger, independent context (TestnetTimeout)
		// once it learns a claim was for a testnet-mode payment, rather
		// than this loop needing to know the mode up front (review Finding
		// 4).
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

func envBigInt(key string, def *big.Int) *big.Int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, ok := new(big.Int).SetString(v, 10)
	if !ok {
		log.Fatalf("invalid %s: not a valid base-10 integer", key)
	}
	return n
}

func envInt64(key string, def int64) int64 {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		log.Fatalf("invalid %s: %v", key, err)
	}
	return n
}
