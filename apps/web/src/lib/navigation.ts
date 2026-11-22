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
    summary: "Persisted ingestion counts from PostgreSQL.",
  },
  {
    href: "/data-sources",
    label: "Data Sources",
    icon: Database,
    available: false,
    summary:
      "Kafka, PostgreSQL, ClickHouse, and MinIO are provisioned locally. This page does not query them, and ClickHouse and MinIO have no application datasets yet.",
  },
  {
    href: "/streaming",
    label: "Streaming",
    icon: Radio,
    available: false,
    summary:
      "The order consumer is a separate service. Broker lag, throughput, and partition detail are not collected, so none are shown here.",
  },
  {
    href: "/pipelines",
    label: "Pipelines",
    icon: GitBranch,
    available: false,
    summary: "Orchestrated pipelines are a later milestone. No pipeline runs are recorded.",
  },
  {
    href: "/warehouse",
    label: "Warehouse",
    icon: Warehouse,
    available: false,
    summary:
      "ClickHouse is running as infrastructure only. TITAN has not created warehouse tables or loaded events into it.",
  },
  {
    href: "/data-quality",
    label: "Data Quality",
    icon: ShieldCheck,
    available: false,
    summary:
      "Quarantine counts are on Overview. A data-quality workspace is not available yet.",
  },
  {
    href: "/ml",
    label: "ML & Predictions",
    icon: Brain,
    available: false,
    summary: "No models are trained or served. This module is not available.",
  },
  {
    href: "/monitoring",
    label: "Monitoring",
    icon: Activity,
    available: false,
    summary: "Host and broker monitoring is not wired up. No latency or error-rate series exist.",
  },
];
