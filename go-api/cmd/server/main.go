package main

import (
	"context"
	"database/sql"
	"flag"
	"log/slog"
	"math/big"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/ethereum/go-ethereum/common"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"chainroute/go-api/internal/bridge/across"
	"chainroute/go-api/internal/bridge/quote"
	"chainroute/go-api/internal/bridge/relay"
	"chainroute/go-api/internal/grpcclient"
	"chainroute/go-api/internal/handler"
	"chainroute/go-api/internal/observability"
	"chainroute/go-api/internal/postgres"
)

func main() {
	httpAddr := flag.String("http-addr", ":8080", "HTTP listen address")
	grpcAddr := flag.String("grpc-addr", "127.0.0.1:50051", "gRPC routing service address")
	flag.Parse()

	logger := observability.NewLogger("go-api")

	shutdownTracing, err := observability.InitTracing(context.Background(), "go-api")
	if err != nil {
		logger.Warn("tracing initialization failed, continuing without traces", "error", err)
	}
	if shutdownTracing == nil {
		// Defensive only: InitTracing never actually returns a nil
		// shutdown today (it fails open with a no-op shutdown func even
		// on error), but nothing enforces that invariant across future
		// edits, and the deferred call below would nil-panic if it ever
		// did.
		shutdownTracing = func(context.Context) error { return nil }
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := shutdownTracing(shutdownCtx); err != nil {
			logger.Warn("tracing shutdown error", "error", err)
		}
	}()

	metrics := observability.NewMetrics()

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		logger.Error("DATABASE_URL environment variable is required")
		os.Exit(1)
	}

	blockchainEnv := os.Getenv("BLOCKCHAIN_ENV")
	maxTestnetAmountWei := envBigIntServer("MAX_TESTNET_AMOUNT_WEI", big.NewInt(10_000_000_000_000_000), logger)

	var registry *quote.Registry
	if blockchainEnv == "testnet" {
		acrossBaseURL := envOrDefaultServer("ACROSS_TESTNET_API_URL", "https://testnet.across.to/api")
		relayBaseURL := envOrDefaultServer("RELAY_TESTNET_API_URL", "https://api.testnets.relay.link")
		routingQuoteTTL := envDurationServer("ROUTING_QUOTE_TTL_SECONDS", 2*time.Minute, time.Second, logger)
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
		logger.Error("failed to dial routing service", "grpc_addr", *grpcAddr, "error", err)
		os.Exit(1)
	}
	defer client.Close()

	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		logger.Error("failed to open database", "error", err)
		os.Exit(1)
	}
	defer db.Close()
	db.SetMaxOpenConns(10)

	pingCtx, pingCancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := db.PingContext(pingCtx); err != nil {
		pingCancel()
		logger.Error("failed to connect to database", "error", err)
		os.Exit(1)
	}
	pingCancel()

	store := postgres.New(db)

	h := &handler.Handler{
		Client: client, Store: store, DashboardStore: store, BlockchainEnv: blockchainEnv,
		MaxTestnetAmountWei: maxTestnetAmountWei, QuoteRegistry: registry,
		Metrics: metrics, Logger: logger,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /routes", h.PostRoutes)
	mux.HandleFunc("POST /payments", h.PostPayments)
	mux.HandleFunc("GET /payments", h.ListPayments)
	mux.HandleFunc("GET /payments/{id}", h.GetPayment)
	mux.HandleFunc("GET /payments/{id}/quotes", h.GetPaymentQuotes)
	mux.HandleFunc("GET /dashboard/stats", h.GetDashboardStats)
	mux.HandleFunc("GET /dashboard/timeseries", h.GetDashboardTimeseries)
	mux.Handle("GET /metrics", promhttp.HandlerFor(metrics.Registry, promhttp.HandlerOpts{}))

	// CORS wraps the whole mux (before otelhttp instrumentation) so its
	// headers apply to every route, including /metrics. This API has no
	// cookies/credentials, and the allowed-origin list is explicit and
	// configured -- never "*".
	corsOrigins := strings.Split(envOrDefaultServer("CHAINROUTE_CORS_ALLOWED_ORIGINS", "http://localhost:5173"), ",")
	corsHandler := handler.CORS(corsOrigins, mux)
	instrumentedMux := otelhttp.NewHandler(corsHandler, "http.server", otelhttp.WithSpanNameFormatter(spanNameFormatter))
	server := &http.Server{Addr: *httpAddr, Handler: instrumentedMux}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		logger.Info("go-api listening", "http_addr", *httpAddr, "grpc_addr", *grpcAddr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("HTTP server error", "error", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	logger.Info("shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Warn("graceful shutdown error", "error", err)
	}
	logger.Info("go-api shut down")
}

func envBigIntServer(key string, def *big.Int, logger *slog.Logger) *big.Int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, ok := new(big.Int).SetString(v, 10)
	if !ok {
		logger.Error("invalid environment variable: not a valid base-10 integer", "key", key)
		os.Exit(1)
	}
	return n
}

func envOrDefaultServer(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envDurationServer(key string, def time.Duration, unit time.Duration, logger *slog.Logger) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		logger.Error("invalid environment variable", "key", key, "error", err)
		os.Exit(1)
	}
	return time.Duration(n) * unit
}

// spanNameFormatter names each HTTP server span by its bounded route
// pattern (e.g. "GET /payments/{id}"), not the raw request path -- using
// r.URL.Path would give GET /payments/{id} a distinct span name per
// payment UUID, trading the original "everything is named http.server"
// cardinality problem for a worse one in Jaeger's operation index.
// r.Pattern is populated by net/http's ServeMux from the registered
// pattern (e.g. "GET /payments/{id}") once routing has matched; it is
// empty on the pre-routing call otelhttp itself makes before dispatching
// to the mux, hence the fallback.
func spanNameFormatter(_ string, r *http.Request) string {
	if r.Pattern != "" {
		return r.Pattern
	}
	return r.Method + " " + r.URL.Path
}
