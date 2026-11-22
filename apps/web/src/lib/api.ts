import type { HealthReady, IngestionStats, RecentEventsResponse } from "@/lib/types";

export const API_BASE_URL = process.env.NEXT_PUBLIC_API_BASE_URL ?? "http://localhost:8000";

export class ApiError extends Error {
  constructor(
    message: string,
    readonly status: number,
  ) {
    super(message);
    this.name = "ApiError";
  }
}

export async function getJson<T>(path: string): Promise<T> {
  const response = await fetch(`${API_BASE_URL}${path}`, { cache: "no-store" });
  const text = await response.text();
  if (!response.ok) {
    throw new ApiError(text || `HTTP ${response.status}`, response.status);
  }
  return JSON.parse(text) as T;
}

export function fetchOverview() {
  return Promise.all([
    getJson<HealthReady>("/health/ready"),
    getJson<IngestionStats>("/api/v1/ingestion/stats"),
    getJson<RecentEventsResponse>("/api/v1/events/recent?limit=20"),
  ] as const);
}
