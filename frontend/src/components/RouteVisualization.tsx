import type { ExecutionMode, Hop } from "../api/types";
import { CHAIN_DISPLAY_NAMES, SIMULATED_GRAPH_CHAINS, hasRealTestnetExecution } from "../lib/chains";

export interface RouteVisualizationProps {
  hops: Hop[];
  sourceChain: string;
  destinationChain: string;
  executionMode: ExecutionMode;
  asset: string;
}

// Fixed pentagon layout for the 5 SIMULATED_GRAPH_CHAINS nodes. A pentagon
// (rather than a horizontal row) reads as "a network" and avoids implying
// a linear pipeline -- every chain is structurally reachable from every
// other chain in the simulated graph, which a row layout would obscure.
const VIEW_WIDTH = 400;
const VIEW_HEIGHT = 320;
const CENTER = { x: 200, y: 150 };
const RADIUS = 110;
const NODE_RADIUS = 15;

// `brand-500` from tailwind.config.js -- the same accent StatusBadge's
// sibling components use for "selected/highlighted" (see PaymentTable's
// `text-brand-700` link color). Reused here, not re-picked, so "this
// payment's actual path" reads as the same kind of highlight everywhere
// in the app.
const ACCENT = "#0bb6a3";
const ACCENT_TEXT = "#0b5c58"; // brand-800
// Restrained slate/neutral base (tailwind.config.js's own description of
// its default palette) for everything that is merely "structurally
// routable," not part of this payment.
const MUTED_EDGE = "#cbd5e1"; // slate-300
const MUTED_NODE_FILL = "#f1f5f9"; // slate-100
const MUTED_NODE_STROKE = "#94a3b8"; // slate-400
const MUTED_TEXT = "#64748b"; // slate-500
const DARK_TEXT = "#0f172a"; // slate-900

type Point = { x: number; y: number };

// Positions computed from SIMULATED_GRAPH_CHAINS's own order/length rather
// than hardcoded per-chain coordinates, so this still lays out correctly
// (just relabeled) if lib/chains.ts's chain list ever changes.
function computeNodePositions(): Record<string, Point> {
  const positions: Record<string, Point> = {};
  const count = SIMULATED_GRAPH_CHAINS.length;
  SIMULATED_GRAPH_CHAINS.forEach((chain, index) => {
    const angleDeg = -90 + (index * 360) / count;
    const angleRad = (angleDeg * Math.PI) / 180;
    positions[chain] = {
      x: CENTER.x + RADIUS * Math.cos(angleRad),
      y: CENTER.y + RADIUS * Math.sin(angleRad),
    };
  });
  return positions;
}

const NODE_POSITIONS = computeNodePositions();

function edgeKey(a: string, b: string): string {
  return [a, b].sort().join("::");
}

function chainLabel(chain: string): string {
  return CHAIN_DISPLAY_NAMES[chain] ?? chain;
}

// Every unordered pair among the 5 simulated-graph chains -- the full set
// of edges that are "structurally routable in the simulated graph,"
// independent of what this particular payment's real hops traversed.
const ALL_GRAPH_EDGES: Array<[string, string]> = SIMULATED_GRAPH_CHAINS.flatMap((chain, i) =>
  SIMULATED_GRAPH_CHAINS.slice(i + 1).map((other): [string, string] => [chain, other]),
);

// Reusable SVG network diagram of the 5 simulated-mode chains, highlighting
// the specific path a single payment's real `hops` array actually took.
// Renders only chains/edges present in `hops` -- it never fabricates an
// intermediate hop, and never implies real execution beyond the one
// corridor `hasRealTestnetExecution` reports.
export function RouteVisualization({
  hops,
  sourceChain,
  destinationChain,
  executionMode,
  asset,
}: RouteVisualizationProps) {
  // Single source of truth for "simulated" vs. "real testnet execution"
  // lives in lib/chains.ts -- this component calls it directly rather
  // than re-deriving the same judgment from chain names itself.
  const isLive = hasRealTestnetExecution(sourceChain, destinationChain, asset, executionMode);

  const traversedNodeIds = new Set<string>();
  const traversedEdgeKeys = new Set<string>();
  for (const hop of hops) {
    traversedNodeIds.add(hop.from_chain);
    traversedNodeIds.add(hop.to_chain);
    traversedEdgeKeys.add(edgeKey(hop.from_chain, hop.to_chain));
  }

  const orderedHops = hops.slice().sort((a, b) => a.hop_index - b.hop_index);

  return (
    <div>
      <div className="mb-3 flex items-center justify-between">
        <h3 className="text-sm font-medium text-slate-700">Route</h3>
        {isLive ? (
          <span className="inline-flex items-center gap-1.5 rounded-full bg-brand-100 px-2.5 py-0.5 text-xs font-medium text-brand-800">
            <svg aria-hidden="true" viewBox="0 0 12 12" className="h-3 w-3 fill-brand-600">
              <path d="M6.5 0 1 7h3.2L4 12l5.5-7H6.3z" />
            </svg>
            Live testnet execution
          </span>
        ) : (
          <span className="inline-flex items-center gap-1.5 rounded-full bg-slate-100 px-2.5 py-0.5 text-xs font-medium text-slate-600">
            Simulated route
          </span>
        )}
      </div>

      <svg
        viewBox={`0 0 ${VIEW_WIDTH} ${VIEW_HEIGHT}`}
        role="img"
        aria-label={`Route diagram from ${chainLabel(sourceChain)} to ${chainLabel(destinationChain)}`}
        className="w-full max-w-md"
      >
        {/* Every potential direct edge in the simulated graph, faint and
            dashed by default -- except the ones this payment's real hops
            actually traversed, which are drawn solid below instead. */}
        {ALL_GRAPH_EDGES.map(([a, b]) => {
          if (traversedEdgeKeys.has(edgeKey(a, b))) return null;
          const pa = NODE_POSITIONS[a];
          const pb = NODE_POSITIONS[b];
          return (
            <line
              key={`${a}-${b}`}
              x1={pa.x}
              y1={pa.y}
              x2={pb.x}
              y2={pb.y}
              stroke={MUTED_EDGE}
              strokeWidth={1}
              strokeDasharray="4 4"
            />
          );
        })}

        {/* This payment's actual hops -- rendered as distinct solid,
            labeled segments in sequence, never collapsed into a single
            source-to-destination arrow, so a genuine multi-hop route
            (hops.length > 1) still shows each intermediate leg. */}
        {orderedHops.map((hop) => {
          const from = NODE_POSITIONS[hop.from_chain];
          const to = NODE_POSITIONS[hop.to_chain];
          // Defensive: never draw a fabricated edge for a chain outside
          // the fixed 5-node graph -- only render what's really there.
          if (!from || !to) return null;
          const midX = (from.x + to.x) / 2;
          const midY = (from.y + to.y) / 2;
          return (
            <g key={hop.hop_index}>
              <line x1={from.x} y1={from.y} x2={to.x} y2={to.y} stroke={ACCENT} strokeWidth={3} />
              <text
                x={midX}
                y={midY - 8}
                textAnchor="middle"
                fontSize={11}
                fontWeight={600}
                fill={ACCENT_TEXT}
                stroke="white"
                strokeWidth={3}
                paintOrder="stroke"
              >
                {hop.bridge_name}
              </text>
            </g>
          );
        })}

        {/* The 5 SIMULATED_GRAPH_CHAINS nodes: muted gray by default,
            accent-highlighted when this payment's path actually visits
            them. */}
        {SIMULATED_GRAPH_CHAINS.map((chain) => {
          const pos = NODE_POSITIONS[chain];
          const onPath = traversedNodeIds.has(chain);
          return (
            <g key={chain}>
              <circle
                cx={pos.x}
                cy={pos.y}
                r={NODE_RADIUS}
                fill={onPath ? ACCENT : MUTED_NODE_FILL}
                stroke={onPath ? ACCENT : MUTED_NODE_STROKE}
                strokeWidth={2}
              />
              <text
                x={pos.x}
                y={pos.y + NODE_RADIUS + 16}
                textAnchor="middle"
                fontSize={12}
                fontWeight={onPath ? 600 : 400}
                fill={onPath ? DARK_TEXT : MUTED_TEXT}
              >
                {chainLabel(chain)}
              </text>
            </g>
          );
        })}
      </svg>

      {/* Legend distinguishing the payment's real path from the always-drawn
          5-node pentagon's structural connectivity, so a viewer never
          mistakes "simulated routing support across the graph" for this
          payment's actual route. Plain HTML swatches (not SVG <line>
          elements) so this doesn't interfere with the diagram-only
          line[stroke]/line[stroke-dasharray] queries the existing tests
          run against the SVG above. */}
      <div className="mt-2 flex flex-col gap-1 text-xs text-slate-500">
        <div className="flex items-center gap-2">
          <span aria-hidden="true" className="h-0.5 w-6 flex-shrink-0" style={{ backgroundColor: ACCENT }} />
          <span>This payment&apos;s route</span>
        </div>
        <div className="flex items-center gap-2">
          <span
            aria-hidden="true"
            className="h-0 w-6 flex-shrink-0 border-t border-dashed"
            style={{ borderColor: MUTED_EDGE }}
          />
          <span>Other routes in the simulated graph</span>
        </div>
      </div>

      {orderedHops.length > 0 && (
        <ol className="mt-3 space-y-1 text-sm text-slate-700">
          {orderedHops.map((hop) => (
            <li key={hop.hop_index} className="flex items-center gap-1.5">
              <span className="font-medium text-slate-500">{hop.hop_index + 1}.</span>
              <span>{chainLabel(hop.from_chain)}</span>
              <span aria-hidden="true">{"→"}</span>
              <span>{chainLabel(hop.to_chain)}</span>
              <span className="text-slate-500">via {hop.bridge_name}</span>
            </li>
          ))}
        </ol>
      )}
    </div>
  );
}
