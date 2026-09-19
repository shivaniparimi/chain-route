# syntax=docker/dockerfile:1
FROM node:20-bookworm AS builder
WORKDIR /src
COPY package.json package-lock.json ./
RUN npm ci
COPY . .
RUN npm run build

FROM nginx:1.27-alpine AS runtime
COPY --from=builder /src/dist /usr/share/nginx/html
COPY docker-entrypoint.sh /docker-entrypoint.d/40-chainroute-env-config.sh
RUN chmod +x /docker-entrypoint.d/40-chainroute-env-config.sh
EXPOSE 80
# wget is provided by busybox, which is part of the alpine base nginx:1.27-alpine
# is built from -- it ships by default and needs no extra `apk add`, unlike curl
# which is not installed in the base alpine image. This has NOT been confirmed
# by an actual `docker build` / container run (no reachable Docker daemon in the
# environment this was authored in) -- see task-13-report.md for the full
# verification trail and confidence level on this specific choice.
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD wget --spider -q http://localhost:80/ || exit 1
