#!/bin/sh
set -eu
echo "window.__CHAINROUTE_API_BASE_URL__ = \"${API_BASE_URL:-http://localhost:8080}\";" > /usr/share/nginx/html/env-config.js
