package main

import (
	"context"
	"database/sql"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

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

	h := &handler.Handler{Client: client, Store: store}
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
