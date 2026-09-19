# ChainRoute payment analytics dashboard

Read-only React/TypeScript dashboard over the go-api's payment/dashboard
endpoints (`GET /payments`, `GET /payments/{id}`, `GET /payments/{id}/quotes`,
`GET /dashboard/stats`, `GET /dashboard/timeseries`).

Stack: Vite + React + TypeScript, Tailwind CSS, TanStack Query, Recharts,
React Router.

## Development

```bash
npm install
npm run dev
```

The API base URL is environment-configurable, checked in this order:

1. `window.__CHAINROUTE_API_BASE_URL__`, set by `public/env-config.js`
   (overwritten at container start by the Docker entrypoint in
   deployed environments).
2. `VITE_API_BASE_URL` (a `.env.local` value, for local dev against a
   non-default API host/port).
3. `http://localhost:8080`, the default.

## Verification

```bash
npm run build        # tsc -b && vite build
npx tsc --noEmit
npx eslint src --ext .ts,.tsx
```
