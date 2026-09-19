declare global {
  interface Window {
    __CHAINROUTE_API_BASE_URL__?: string;
  }
}

function apiBaseUrl(): string {
  if (typeof window !== "undefined" && window.__CHAINROUTE_API_BASE_URL__) {
    return window.__CHAINROUTE_API_BASE_URL__;
  }
  return import.meta.env.VITE_API_BASE_URL ?? "http://localhost:8080";
}

export class ApiError extends Error {
  // Not a constructor parameter property (`public status: number` in the
  // constructor signature) -- this project's tsconfig.app.json sets
  // erasableSyntaxOnly, which rejects parameter properties because they
  // require the compiler to emit an extra `this.status = status`
  // assignment rather than purely erasing types. Declaring the field and
  // assigning it in the body is erasable-safe and behaves identically.
  status: number;

  constructor(status: number, message: string) {
    super(message);
    this.name = "ApiError";
    this.status = status;
  }
}

export async function apiGet<T>(
  path: string,
  params?: Record<string, string | number | undefined>,
): Promise<T> {
  const url = new URL(apiBaseUrl() + path);
  if (params) {
    for (const [key, value] of Object.entries(params)) {
      if (value !== undefined) url.searchParams.set(key, String(value));
    }
  }
  const res = await fetch(url.toString());
  if (!res.ok) {
    let message = res.statusText;
    try {
      const body = await res.json();
      if (body?.error) message = body.error;
    } catch {
      // ignore -- fall back to statusText
    }
    throw new ApiError(res.status, message);
  }
  return res.json() as Promise<T>;
}
