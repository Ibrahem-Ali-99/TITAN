export type HealthReady = {
  status: string;
  checks: Record<string, string>;
};

export type ActivityPoint = {
  minute: string;
  persisted_events: number;
};

export type IngestionStats = {
  persisted_events: number;
  persisted_order_created_events: number;
  quarantined_records: number;
  most_recent_ingested_at: string | null;
  recent_activity: ActivityPoint[];
  activity_window_minutes: number;
  metric_definitions: Record<string, string>;
};

export type RecentEvent = {
  event_id: string;
  event_type: string;
  schema_version: number;
  order_id: string;
  occurred_at: string;
  ingested_at: string;
  source: string;
  currency: string | null;
  total_amount: string | null;
  synthetic: boolean;
  kafka_topic: string;
  kafka_partition: number;
  kafka_offset: number;
};

export type RecentEventsResponse = {
  items: RecentEvent[];
  limit: number;
  returned: number;
};
