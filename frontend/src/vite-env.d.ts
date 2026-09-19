/// <reference types="vite/client" />

// VITE_API_BASE_URL is not one of Vite's built-in env vars, so
// ImportMetaEnv needs augmenting for src/api/client.ts's
// `import.meta.env.VITE_API_BASE_URL` fallback (used only in non-Docker
// dev, when window.__CHAINROUTE_API_BASE_URL__ from public/env-config.js
// is absent) to type-check.
interface ImportMetaEnv {
  readonly VITE_API_BASE_URL?: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
