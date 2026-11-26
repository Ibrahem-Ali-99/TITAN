"use client";

import {
  Bar,
  BarChart,
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

const grid = "#26394b";
const axis = "#8ea0b5";
const tooltipStyle = {
  background: "#111d2b",
  border: "1px solid #26394b",
  borderRadius: 8,
  fontSize: 12,
};

export function ActivityChart({
  points,
}: {
  points: { label: string; persisted: number }[];
}) {
  const peak = points.reduce((max, point) => Math.max(max, point.persisted), 0);
  return (
    <div>
      <div className="h-52" role="img" aria-label="Persisted events by minute">
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={points} margin={{ top: 8, right: 8, left: 0, bottom: 0 }}>
            <CartesianGrid stroke={grid} vertical={false} />
            <XAxis dataKey="label" stroke={axis} fontSize={11} tickLine={false} />
            <YAxis stroke={axis} fontSize={11} allowDecimals={false} tickLine={false} width={32} />
            <Tooltip contentStyle={tooltipStyle} />
            <Bar dataKey="persisted" name="Persisted events" fill="#2dd4bf" radius={[3, 3, 0, 0]} maxBarSize={26} />
          </BarChart>
        </ResponsiveContainer>
      </div>
      <p className="mt-2 text-xs text-[var(--muted)]">{peak > 0 ? `${peak} peak` : "No observed events in this window"}</p>
    </div>
  );
}

export function CurrencyOrdersChart({
  points,
}: {
  points: { label: string; EUR: number; GBP: number; USD: number }[];
}) {
  return (
    <div className="h-64" role="img" aria-label="Accepted order counts by currency over time">
      <ResponsiveContainer width="100%" height="100%">
        <LineChart data={points} margin={{ top: 8, right: 8, left: 0, bottom: 0 }}>
          <CartesianGrid stroke={grid} vertical={false} />
          <XAxis dataKey="label" stroke={axis} fontSize={11} tickLine={false} minTickGap={24} />
          <YAxis stroke={axis} fontSize={11} allowDecimals={false} tickLine={false} width={36} />
          <Tooltip contentStyle={tooltipStyle} />
          <Legend />
          <Line type="monotone" dataKey="EUR" stroke="#2dd4bf" strokeWidth={2} dot={false} />
          <Line type="monotone" dataKey="GBP" stroke="#34d399" strokeWidth={2} dot={false} />
          <Line type="monotone" dataKey="USD" stroke="#f5b942" strokeWidth={2} dot={false} />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}

export function RevenueChart({
  points,
}: {
  points: { label: string; orders: number; revenue: string }[];
}) {
  return (
    <div className="h-64" role="img" aria-label="Accepted orders over time for the selected currency">
      <ResponsiveContainer width="100%" height="100%">
        <LineChart data={points} margin={{ top: 8, right: 8, left: 0, bottom: 0 }}>
          <CartesianGrid stroke={grid} vertical={false} />
          <XAxis dataKey="label" stroke={axis} fontSize={11} tickLine={false} minTickGap={24} />
          <YAxis stroke={axis} fontSize={11} allowDecimals={false} tickLine={false} width={36} />
          <Tooltip
            contentStyle={tooltipStyle}
            formatter={(value, _name, item) => {
              const payload = item.payload as { revenue?: string };
              return [`${value} orders · ${payload.revenue ?? ""}`, "Accepted orders"];
            }}
          />
          <Line type="monotone" dataKey="orders" name="Accepted orders" stroke="#3b82f6" strokeWidth={2} dot={false} />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}

export function ProductChart({
  points,
}: {
  points: { product: string; quantity: number; revenue: string }[];
}) {
  return (
    <div className="h-64" role="img" aria-label="Quantity sold by product for the selected currency">
      <ResponsiveContainer width="100%" height="100%">
        <BarChart data={points} margin={{ top: 8, right: 8, left: 0, bottom: 40 }}>
          <CartesianGrid stroke={grid} vertical={false} />
          <XAxis dataKey="product" stroke={axis} fontSize={11} interval={0} angle={-25} textAnchor="end" height={60} />
          <YAxis stroke={axis} fontSize={11} allowDecimals={false} tickLine={false} width={36} />
          <Tooltip
            contentStyle={tooltipStyle}
            formatter={(value, _name, item) => {
              const payload = item.payload as { revenue?: string };
              return [`${value} units · ${payload.revenue ?? ""}`, "Quantity"];
            }}
          />
          <Bar dataKey="quantity" name="Quantity" fill="#2dd4bf" radius={[3, 3, 0, 0]} />
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}

export function MetricCompareChart({
  rows,
  candidateName = "Candidate",
}: {
  rows: { metric: string; baseline: number; candidate: number }[];
  candidateName?: string;
}) {
  return (
    <div className="h-56" role="img" aria-label="Baseline and candidate error comparison">
      <ResponsiveContainer width="100%" height="100%">
        <BarChart data={rows} margin={{ top: 8, right: 8, left: 0, bottom: 0 }}>
          <CartesianGrid stroke={grid} vertical={false} />
          <XAxis dataKey="metric" stroke={axis} fontSize={12} tickLine={false} />
          <YAxis stroke={axis} fontSize={11} tickLine={false} width={48} />
          <Tooltip contentStyle={tooltipStyle} />
          <Legend />
          <Bar dataKey="baseline" name="Seasonal naive" fill="#34d399" radius={[3, 3, 0, 0]} />
          <Bar dataKey="candidate" name={candidateName} fill="#3b82f6" radius={[3, 3, 0, 0]} />
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}

export function EvaluationChart({
  points,
}: {
  points: { date: string; actual: number | null; predicted: number }[];
}) {
  return (
    <div className="h-64" role="img" aria-label="One-step evaluation, actual quantity versus predicted quantity">
      <ResponsiveContainer width="100%" height="100%">
        <LineChart data={points} margin={{ top: 8, right: 8, left: 0, bottom: 0 }}>
          <CartesianGrid stroke={grid} vertical={false} />
          <XAxis dataKey="date" stroke={axis} fontSize={11} tickLine={false} minTickGap={16} />
          <YAxis stroke={axis} fontSize={11} tickLine={false} width={36} />
          <Tooltip contentStyle={tooltipStyle} />
          <Legend />
          <Line type="monotone" dataKey="actual" name="Actual" stroke="#2dd4bf" strokeWidth={2} dot={{ r: 2 }} connectNulls={false} />
          <Line type="monotone" dataKey="predicted" name="Predicted" stroke="#60a5fa" strokeWidth={2} dot={{ r: 2 }} />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}

export function RunDurationChart({ points }: { points: { label: string; duration_ms: number }[] }) {
  return (
    <div className="h-56" role="img" aria-label="Observed pipeline run durations in milliseconds">
      <ResponsiveContainer width="100%" height="100%">
        <BarChart data={points} margin={{ top: 8, right: 8, left: 0, bottom: 0 }}>
          <CartesianGrid stroke={grid} vertical={false} />
          <XAxis dataKey="label" stroke={axis} fontSize={11} tickLine={false} interval={0} angle={-18} textAnchor="end" height={42} />
          <YAxis stroke={axis} fontSize={11} tickLine={false} width={48} />
          <Tooltip contentStyle={tooltipStyle} formatter={(value) => [`${value} ms`, "Duration"]} />
          <Bar dataKey="duration_ms" name="Duration" fill="#60a5fa" radius={[3, 3, 0, 0]} maxBarSize={30} />
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}
