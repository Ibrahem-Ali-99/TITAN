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

const grid = "#27272a";
const axis = "#a1a1aa";
const tooltipStyle = {
  background: "#12151c",
  border: "1px solid #3f3f46",
  borderRadius: 8,
  fontSize: 12,
};

export function ActivityChart({
  points,
}: {
  points: { label: string; persisted: number }[];
}) {
  return (
    <div className="h-56" role="img" aria-label="Persisted events by minute">
      <ResponsiveContainer width="100%" height="100%">
        <LineChart data={points} margin={{ top: 8, right: 8, left: 0, bottom: 0 }}>
          <CartesianGrid stroke={grid} vertical={false} />
          <XAxis dataKey="label" stroke={axis} fontSize={11} tickLine={false} />
          <YAxis stroke={axis} fontSize={11} allowDecimals={false} tickLine={false} width={32} />
          <Tooltip contentStyle={tooltipStyle} />
          <Line
            type="monotone"
            dataKey="persisted"
            name="Persisted events"
            stroke="#3b82f6"
            strokeWidth={2}
            dot={false}
          />
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
          <Bar dataKey="quantity" name="Quantity" fill="#3b82f6" radius={[3, 3, 0, 0]} />
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
          <Line type="monotone" dataKey="actual" name="Actual" stroke="#e4e4e7" strokeWidth={2} dot={false} connectNulls={false} />
          <Line type="monotone" dataKey="predicted" name="Predicted" stroke="#3b82f6" strokeWidth={2} dot={false} />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}
