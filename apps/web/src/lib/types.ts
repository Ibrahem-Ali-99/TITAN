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
  kafka_consumed_events: number;
  logical_orders: number;
  order_conflicts: number;
  duplicate_operations: number;
  duplicate_deliveries: number;
  accepted_payments: number;
  payment_conflicts: number;
  inventory_updates: number;
  rest_events_published: number;
  rest_events_rejected: number;
  csv_rows_accepted: number;
  csv_rows_rejected: number;
  csv_rows_publish_failed: number;
  replays_published: number;
  replays_refused: number;
  most_recent_ingested_at: string | null;
  most_recent_error_at: string | null;
  recent_activity: ActivityPoint[];
  activity_window_minutes: number;
  metric_definitions: Record<string, string>;
};

export type RecentEvent = {
  event_id: string;
  event_type: string;
  schema_version: number;
  order_id: string | null;
  disposition: string;
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

export type SourceSubmissionList = {
  returned: number;
  items: {
    id: string;
    channel: string;
    received_at: string;
    status: string;
    event_id: string | null;
    event_type: string | null;
  }[];
};

export type CsvImportList = {
  returned: number;
  items: {
    import_id: string;
    filename: string;
    status: string;
    accepted_rows: number;
    rejected_rows: number;
    publish_failed_rows: number;
    received_at: string;
  }[];
};

export type QuarantineList = {
  returned: number;
  items: {
    id: number;
    received_at: string;
    reason_code: string;
    replay_status: string;
    kafka_topic: string;
    kafka_partition: number;
    kafka_offset: number;
  }[];
};

export type ReplayList = {
  returned: number;
  items: {
    id: string;
    requested_at: string;
    mode: string;
    outcome: string;
    downstream_state: string;
    detail: string;
  }[];
};
