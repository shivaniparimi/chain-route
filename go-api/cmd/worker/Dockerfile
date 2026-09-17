# syntax=docker/dockerfile:1
FROM golang:1.27-bookworm AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/worker ./cmd/worker

FROM gcr.io/distroless/static-debian12:nonroot AS runtime
COPY --from=builder /out/worker /app/worker
EXPOSE 9091
ENTRYPOINT ["/app/worker"]
