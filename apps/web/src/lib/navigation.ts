import {
  Activity,
  Brain,
  Database,
  Gauge,
  GitBranch,
  Radio,
  ShieldCheck,
  Warehouse,
  type LucideIcon,
} from "lucide-react";

export type NavItem = {
  href: string;
  label: string;
  icon: LucideIcon;
  available: boolean;
  summary: string;
};

export const NAV_ITEMS: NavItem[] = [
  {
    href: "/",
    label: "Overview",
    icon: Gauge,
    available: true,
    summary: "Live ingestion counts beside the last published analytical snapshot.",
  },
  {
    href: "/data-sources",
    label: "Data Sources",
    icon: Database,
    available: true,
    summary: "REST submissions, CSV imports, and the synthetic generator.",
  },
  {
    href: "/streaming",
    label: "Streaming",
    icon: Radio,
    available: true,
    summary: "Durable consume, quarantine, conflict, and replay outcomes.",
  },
  {
    href: "/pipelines",
    label: "Pipelines",
    icon: GitBranch,
    available: true,
    summary: "Spark publications and recorded warehouse run attempts.",
  },
  {
    href: "/warehouse",
    label: "Warehouse",
    icon: Warehouse,
    available: true,
    summary: "The active ClickHouse publication and its synthetic commerce metrics.",
  },
  {
    href: "/data-quality",
    label: "Data Quality",
    icon: ShieldCheck,
    available: true,
    summary: "Candidate checks, findings, and whether a run was an injected fault.",
  },
  {
    href: "/ml",
    label: "ML & Predictions",
    icon: Brain,
    available: true,
    summary: "Stored demand forecasts, baseline metrics, and statistical anomalies.",
  },
  {
    href: "/monitoring",
    label: "Monitoring",
    icon: Activity,
    available: true,
    summary: "Pipeline history, freshness, and ingestion-consumer lag.",
  },
];
