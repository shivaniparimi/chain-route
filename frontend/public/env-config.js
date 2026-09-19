// Committed placeholder/default. Overwritten at container start by Task
// 13's Docker entrypoint with the deployment's real API base URL. Loaded
// by index.html BEFORE the main Vite bundle script tag, so
// src/api/client.ts's apiBaseUrl() can read window.__CHAINROUTE_API_BASE_URL__
// synchronously on first use.
window.__CHAINROUTE_API_BASE_URL__ = "http://localhost:8080";
