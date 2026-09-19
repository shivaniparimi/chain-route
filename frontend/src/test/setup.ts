// Vitest test setup: the `/vitest` subpath both registers jest-dom's
// matchers on vitest's own `expect` (via `expect.extend(...)`, importing
// `expect` from "vitest" directly -- no ambient globals required) and
// declaration-merges the matcher types onto vitest's `Assertion`
// interface, so `expect(el).toBeInTheDocument()` type-checks.
import "@testing-library/jest-dom/vitest";

import { cleanup } from "@testing-library/react";
import { afterEach } from "vitest";

// @testing-library/react's own auto-cleanup only registers itself when
// it detects a global `afterEach` (i.e. with vitest's `test.globals:
// true`). This project doesn't set that, so unmount explicitly after
// every test -- otherwise each test file's later tests see every
// previous test's still-mounted DOM, producing false "multiple
// elements found" failures.
afterEach(() => {
  cleanup();
});
