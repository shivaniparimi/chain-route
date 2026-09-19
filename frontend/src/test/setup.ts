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

// Recharts' <ResponsiveContainer> (used by every chart in
// src/components/charts, Task 12) measures its parent via ResizeObserver
// and renders nothing (null) until it has a positive width/height --
// see recharts' ResponsiveContainer.js, which explicitly no-ops
// (`typeof ResizeObserver === 'undefined'`) rather than throwing when the
// global is absent, exactly jsdom's default. Without this polyfill every
// chart test would see an empty container regardless of the data passed
// in. This is a minimal stub (not a full ResizeObserver -- it never
// actually fires resize callbacks), sufficient because ResponsiveContainer
// also reads the container's getBoundingClientRect() once synchronously
// when it constructs the observer, which the jsdom stub below satisfies.
class ResizeObserverStub {
  observe() {}
  unobserve() {}
  disconnect() {}
}
globalThis.ResizeObserver = ResizeObserverStub;

// jsdom's layout engine doesn't compute real box sizes, so
// getBoundingClientRect() always returns all-zero dimensions -- which
// recharts' ResponsiveContainer treats as "not acceptable" and renders
// null. Stubbing a fixed, positive size lets every chart under test
// actually render its SVG content.
//
// Scoped to exactly the `.recharts-responsive-container` element (the one
// node ResponsiveContainer itself measures, per its own source) rather than
// every element: recharts' axis tick-reduction logic also calls
// getBoundingClientRect on individual tick <text>/<tspan> nodes to estimate
// label width, and a blanket 600x240 override there makes every tick look
// enormous, collapsing a 3-point axis down to a single visible tick. Every
// other element keeps jsdom's real (zero-size) implementation.
const realGetBoundingClientRect = Element.prototype.getBoundingClientRect;
Element.prototype.getBoundingClientRect = function (this: Element) {
  if (this.classList.contains("recharts-responsive-container")) {
    return {
      width: 600,
      height: 240,
      top: 0,
      left: 0,
      bottom: 240,
      right: 600,
      x: 0,
      y: 0,
      toJSON() {},
    } as DOMRect;
  }
  return realGetBoundingClientRect.call(this);
};
