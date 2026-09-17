package main

import (
	"context"
	"database/sql"
	"flag"
	"log"
	"math/big"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/ethereum/go-ethereum/common"
	_ "github.com/jackc/pgx/v5/stdlib"

	"chainroute/go-api/internal/bridge/across"
	"chainroute/go-api/internal/bridge/quote"
	"chainroute/go-api/internal/bridge/relay"
	"chainroute/go-api/internal/grpcclient"
	"chainroute/go-api/internal/handler"
	"chainroute/go-api/internal/postgres"
)

func main() {
	httpAddr := flag.String("http-addr", ":8080", "HTTP listen address")
	grpcAddr := flag.String("grpc-addr", "127.0.0.1:50051", "gRPC routing service address")
	flag.Parse()

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		log.Fatal("DATABASE_URL environment variable is required")
	}

	blockchainEnv := os.Getenv("BLOCKCHAIN_ENV")
	maxTestnetAmountWei := envBigIntServer("MAX_TESTNET_AMOUNT_WEI", big.NewInt(10_000_000_000_000_000))

	var registry *quote.Registry
	if blockchainEnv == "testnet" {
		acrossBaseURL := envOrDefaultServer("ACROSS_TESTNET_API_URL", "https://testnet.across.to/api")
		relayBaseURL := envOrDefaultServer("RELAY_TESTNET_API_URL", "https://api.testnets.relay.link")
		routingQuoteTTL := envDurationServer("ROUTING_QUOTE_TTL_SECONDS", 2*time.Minute, time.Second)
		acrossClient := across.NewClient(acrossBaseURL)
		acrossClient.APIKey = os.Getenv("ACROSS_API_KEY")
		acrossClient.IntegratorID = os.Getenv("ACROSS_INTEGRATOR_ID")
		relayClient := relay.NewClient(relayBaseURL)
		relayClient.APIKey = os.Getenv("RELAY_API_KEY")

		// cmd/server never signs anything, so the quoting-only Provider
		// instances here never need a real wallet address -- a zero
		// address is safe (GetQuote uses it only as the quote request's
		// "user" field, which does not need to resolve to funds for a
		// price-discovery call).
		var zeroWallet common.Address
		registry = quote.NewRegistry()
		routeKey := quote.RouteKey{SourceChainID: 11155111, DestinationChainID: 84532, Asset: "WETH"}
		registry.Register(routeKey, across.NewProvider(acrossClient, routingQuoteTTL))
		registry.Register(routeKey, relay.NewProvider(relayClient, zeroWallet, routingQuoteTTL))
	}

	client, err := grpcclient.Dial(*grpcAddr)
	if err != nil {
		log.Fatalf("failed to dial routing service at %s: %v", *grpcAddr, err)
	}
	defer client.Close()

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

	h := &handler.Handler{Client: client, Store: store, BlockchainEnv: blockchainEnv, MaxTestnetAmountWei: maxTestnetAmountWei, QuoteRegistry: registry}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /routes", h.PostRoutes)
	mux.HandleFunc("POST /payments", h.PostPayments)
	mux.HandleFunc("GET /payments/{id}", h.GetPayment)

	server := &http.Server{Addr: *httpAddr, Handler: mux}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		log.Printf("go-api listening on %s, routing service at %s, database connected", *httpAddr, *grpcAddr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("shutting down...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("graceful shutdown error: %v", err)
	}
	log.Println("go-api shut down")
}

func envBigIntServer(key string, def *big.Int) *big.Int {
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

func envOrDefaultServer(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envDurationServer(key string, def time.Duration, unit time.Duration) time.Duration {
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
