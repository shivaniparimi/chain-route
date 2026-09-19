import type { ChangeEvent } from "react";

import type { ExecutionMode, PaymentListFilters, PaymentStatus } from "../api/types";
import { CHAIN_DISPLAY_NAMES, PROVIDER_DISPLAY_NAMES, SIMULATED_GRAPH_CHAINS } from "../lib/chains";
import { STATUS_LABELS } from "./StatusBadge";

export type FilterKey = keyof Pick<
  PaymentListFilters,
  "status" | "provider" | "source_chain" | "destination_chain" | "execution_mode"
>;

// The fixed known-value sets each dropdown is populated from. These are
// deliberately the only selectable values -- every one is guaranteed
// valid against the backend's own filter validation (Task 2), so a
// filtered request built from this UI can never get a 400.
const STATUS_VALUES: PaymentStatus[] = ["ROUTED", "PROCESSING", "SUBMITTED", "COMPLETED", "FAILED"];
const PROVIDER_VALUES = ["across", "relay"] as const;
const EXECUTION_MODE_VALUES: ExecutionMode[] = ["simulated", "testnet"];

interface FilterBarProps {
  filters: PaymentListFilters;
  onChange: (key: FilterKey, value: string | undefined) => void;
}

interface FilterSelectProps {
  label: string;
  filterKey: FilterKey;
  value: string | undefined;
  options: Array<{ value: string; label: string }>;
  onChange: (key: FilterKey, value: string | undefined) => void;
}

function FilterSelect({ label, filterKey, value, options, onChange }: FilterSelectProps) {
  function handleChange(event: ChangeEvent<HTMLSelectElement>) {
    const next = event.target.value;
    onChange(filterKey, next === "" ? undefined : next);
  }

  return (
    <label className="flex flex-col gap-1 text-xs font-medium text-slate-600">
      {label}
      <select
        aria-label={label}
        value={value ?? ""}
        onChange={handleChange}
        className="rounded-md border border-slate-300 bg-white px-2 py-1.5 text-sm text-slate-900 focus:outline-none focus:ring-2 focus:ring-brand-500"
      >
        <option value="">All</option>
        {options.map((option) => (
          <option key={option.value} value={option.value}>
            {option.label}
          </option>
        ))}
      </select>
    </label>
  );
}

// Dropdown filter controls for the payment explorer table (GET
// /payments query params). No free-text inputs: every filterable field
// maps to a fixed, known-valid set of options.
export function FilterBar({ filters, onChange }: FilterBarProps) {
  const chainOptions = SIMULATED_GRAPH_CHAINS.map((chain) => ({
    value: chain,
    label: CHAIN_DISPLAY_NAMES[chain] ?? chain,
  }));

  return (
    <div className="flex flex-wrap items-end gap-3 border-b border-slate-200 bg-white px-4 py-3">
      <FilterSelect
        label="Status"
        filterKey="status"
        value={filters.status}
        onChange={onChange}
        options={STATUS_VALUES.map((status) => ({ value: status, label: STATUS_LABELS[status] }))}
      />
      <FilterSelect
        label="Provider"
        filterKey="provider"
        value={filters.provider}
        onChange={onChange}
        options={PROVIDER_VALUES.map((provider) => ({
          value: provider,
          label: PROVIDER_DISPLAY_NAMES[provider] ?? provider,
        }))}
      />
      <FilterSelect
        label="Source chain"
        filterKey="source_chain"
        value={filters.source_chain}
        onChange={onChange}
        options={chainOptions}
      />
      <FilterSelect
        label="Destination chain"
        filterKey="destination_chain"
        value={filters.destination_chain}
        onChange={onChange}
        options={chainOptions}
      />
      <FilterSelect
        label="Execution mode"
        filterKey="execution_mode"
        value={filters.execution_mode}
        onChange={onChange}
        options={EXECUTION_MODE_VALUES.map((mode) => ({ value: mode, label: mode }))}
      />
    </div>
  );
}
