package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"chainroute/go-api/internal/grpcclient"
	"chainroute/go-api/internal/handler"
)

func main() {
	httpAddr := flag.String("http-addr", ":8080", "HTTP listen address")
	grpcAddr := flag.String("grpc-addr", "127.0.0.1:50051", "gRPC routing service address")
	flag.Parse()

	client, err := grpcclient.Dial(*grpcAddr)
	if err != nil {
		log.Fatalf("failed to dial routing service at %s: %v", *grpcAddr, err)
	}
	defer client.Close()

	h := &handler.Handler{Client: client}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /routes", h.PostRoutes)

	server := &http.Server{Addr: *httpAddr, Handler: mux}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		log.Printf("go-api listening on %s, routing service at %s", *httpAddr, *grpcAddr)
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
