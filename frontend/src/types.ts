export type InferredType = "numeric" | "integer" | "float" | "categorical" | "binary" | "text" | "datetime";
export type AnalysisMode = "rating" | "analog_search";

export type PreviewColumn = {
  source_name: string;
  normalized_name: string;
  inferred_type: InferredType;
  missing_count: number;
  unique_count: number;
  sample_values: unknown[];
};

export type PreviewResponse = {
  filename: string;
  rows_total: number;
  columns: PreviewColumn[];
  preview_rows: Record<string, unknown>[];
  warnings: string[];
  normalized_dataset?: {
    rows: Array<{
      id: string;
      values: Record<string, unknown>;
    }>;
  };
};

export type ChartPoint = {
  label: string;
  value: number;
};

export type FieldRecommendation = {
  code: string;
  severity: "info" | "warning" | "critical" | string;
  message: string;
  suggested_patch: Partial<FieldConfig>;
};

export type FieldProfile = {
  key: string;
  inferred_type: InferredType;
  analytic_candidate: boolean;
  rows_total: number;
  missing_count: number;
  unique_count: number;
  unique_ratio: number;
  sample_values: unknown[];
  numeric_min: number | null;
  numeric_max: number | null;
  numeric_mean: number | null;
  numeric_median: number | null;
  outlier_count_iqr: number;
  histogram: ChartPoint[];
  top_categories: ChartPoint[];
  text_to_categorical_possible: boolean;
  recommended_config: FieldConfig;
  recommendations: FieldRecommendation[];
};

export type DatasetQualityIssue = {
  code: string;
  severity: string;
  message: string;
  affected_fields: string[];
  penalty: number;
};

export type DatasetQualityReport = {
  score: number;
  level: string;
  readiness_label: string;
  analytic_fields_count: number;
  numeric_fields_count: number;
  categorical_fields_count: number;
  text_fields_count: number;
  total_missing_values: number;
  total_outliers_iqr: number;
  issues: DatasetQualityIssue[];
};

export type DatasetProfileResponse = {
  rows_total: number;
  detail_level?: "summary" | "detailed";
  fields: FieldProfile[];
  quality: DatasetQualityReport;
  recommended_weights: Record<string, number>;
  weight_notes: string[];
  missing_matrix_preview: Array<{
    id: string;
    missing_count: number;
    missing_fields: string[];
  }>;
  correlation_matrix: Array<{
    left_key: string;
    right_key: string;
    pearson: number;
    samples: number;
  }>;
};

export type PipelineProfileResponse = {
  dataset_file_id?: number;
  preview: PreviewResponse;
  profile: DatasetProfileResponse;
};

export type PreprocessingRefreshResponse = {
  preview: PreviewResponse;
  profile: DatasetProfileResponse;
  preprocessing_summary: Record<string, unknown>;
};

export type FieldConfig = {
  key: string;
  field_type: InferredType;
  include_in_output: boolean;
  missing_strategy: string;
  missing_constant?: unknown;
  outlier_method: string;
  outlier_threshold: number;
  normalization: string;
  encoding: string;
  rounding_precision?: number | null;
  datetime_format?: string | null;
  ordinal_map?: Record<string, number>;
  binary_map?: Record<string, number>;
};

export type CriterionConfig = {
  key: string;
  name: string;
  weight: number;
  type: "numeric" | "categorical" | "binary";
  direction: "maximize" | "minimize" | "target";
  scale_map?: Record<string, number>;
  target_value?: unknown | null;
};

export type AnalysisFilters = {
  numeric_ranges?: Array<{
    key: string;
    min_value?: number | null;
    max_value?: number | null;
  }>;
  categorical_allowlist?: Array<{
    key: string;
    values: string[];
  }>;
};

export type RankingStabilityScenario = {
  label: string;
  variation_pct: number;
  top_object_id?: string | null;
  changed_positions: number;
  top_n_overlap: number;
  note: string;
};

export type PipelineResult = {
  import_preview: PreviewResponse;
  preprocessing_summary: Record<string, unknown>;
  analysis_summary: Record<string, unknown>;
  history_id?: number | null;
  ranking: Array<{
    object_id: string;
    title: string;
    rank: number;
    score: number;
    similarity_to_target?: number | null;
    explanation: string;
    contributions: Array<{
      key: string;
      name: string;
      raw_value: unknown;
      normalized_value: number;
      weight: number;
      contribution: number;
      note?: string | null;
    }>;
  }>;
};

export type ProjectItem = {
  id: number;
  owner_user_id: number;
  owner_email: string;
  name: string;
  description: string | null;
  status: string;
  metadata_json: string | null;
  created_at: string;
};

export type StorageStats = {
  files_total: number;
  datasets_total: number;
  comparisons_total: number;
  projects_total: number;
  storage_bytes_total: number;
};

export type SystemDashboard = {
  services: Record<string, { status: string; status_code?: number; message?: string }>;
  storage: StorageStats | null;
  auth: AdminStats | null;
  telemetry?: {
    window_size: number;
    overall: {
      requests: number;
      errors: number;
      error_rate_pct: number;
      avg_ms: number;
    };
    modules: Array<{
      module: string;
      requests: number;
      errors: number;
      error_rate_pct: number;
      avg_ms: number;
      p95_ms: number;
    }>;
  };
};

export type AuthUser = {
  id: number;
  email: string;
  full_name: string;
  role: "admin" | "user" | string;
  is_active: boolean;
  created_at: string;
};

export type AuthResponse = {
  access_token: string;
  token_type: string;
  user: AuthUser;
};

export type AdminStats = {
  users_total: number;
  admins_total: number;
  active_users_total: number;
};

export type ComparisonHistoryItem = {
  id: number;
  user_id: number;
  user_email: string;
  title: string;
  source_filename: string | null;
  project_id: number | null;
  version_number: number;
  parent_history_id: number | null;
  dataset_file_id: number | null;
  result_file_id: number | null;
  parameters_json: string;
  summary_json: string;
  tags_json: string | null;
  status: string;
  created_at: string;
};
