import { useEffect, useMemo, useState, type ReactNode } from "react";
import katex from "katex";
import "katex/dist/katex.min.css";
import { TargetObjectPreviewCard } from "./components/TargetObjectPreviewCard";
import {
  bindReportToHistory,
  createProject,
  downloadDocxReport,
  fetchRawObjects,
  fetchStoredProfile,
  fetchAdminStats,
  fetchAdminUsers,
  fetchHistory,
  fetchMe,
  fetchProjects,
  fetchSystemDashboard,
  login,
  profileFile,
  refreshPreprocessing,
  register,
  runPipeline,
  uploadReportFile,
} from "./api";
import type {
  AdminStats,
  AnalysisMode,
  AuthUser,
  ComparisonHistoryItem,
  ChartPoint,
  CriterionConfig,
  FieldProfile,
  FieldConfig,
  PipelineResult,
  PreviewColumn,
  PreviewResponse,
  DatasetQualityReport,
  ProjectItem,
  RawObjectsResponse,
  RankingStabilityScenario,
  SystemDashboard,
} from "./types";

type StageId = "data" | "preprocessing" | "criteria" | "results" | "projects" | "history" | "admin";
type PrepSectionId = "preview" | "types" | "missing" | "outliers" | "encoding" | "scaling";
type ChartMode = "histogram" | "boxplot";
const DATETIME_FORMAT_OPTIONS = [
  "YYYY-MM-DD",
  "YYYY/MM/DD",
  "DD.MM.YYYY",
  "DD-MM-YYYY",
  "YYYY-MM-DD HH:mm:ss",
  "YYYY/MM/DD HH:mm:ss",
  "DD.MM.YYYY HH:mm:ss",
] as const;
const UNIT_OPTIONS_BY_FAMILY: Record<string, Array<{ value: string; label: string }>> = {
  distance: [
    { value: "m", label: "Метры" },
    { value: "km", label: "Километры" },
    { value: "mi", label: "Мили" },
  ],
  weight: [
    { value: "g", label: "Граммы" },
    { value: "kg", label: "Килограммы" },
    { value: "lb", label: "Фунты" },
  ],
};

const stages: Array<{ id: StageId; title: string; caption: string }> = [
  { id: "data", title: "Данные", caption: "Загрузка и предпросмотр" },
  { id: "preprocessing", title: "Подготовка", caption: "Очистка и кодирование" },
  { id: "criteria", title: "Критерии", caption: "Веса и направления" },
  { id: "results", title: "Результаты", caption: "Рейтинг и отчет" },
  { id: "admin", title: "Админ", caption: "Дэшборд системы" },
];

const prepSections: Array<{ id: PrepSectionId; title: string; caption: string }> = [
  { id: "preview", title: "Предпросмотр", caption: "" },
  { id: "types", title: "Типы данных", caption: "" },
  { id: "encoding", title: "Энкодинг", caption: "" },
  { id: "missing", title: "Пропуски", caption: "" },
  { id: "outliers", title: "Выбросы", caption: "" },
  { id: "scaling", title: "Масштабирование", caption: "" },
];

const numericNameHints = ["price", "cost", "area", "rating", "score", "days", "months", "amount", "value"];
const labelFieldHints = ["name", "title", "object_name", "address", "location", "city", "district", "street"];
const geoLatitudeHints = ["latitude", "lat", "широта"];
const geoLongitudeHints = ["longitude", "lon", "lng", "долгота"];

function looksNumeric(column: PreviewColumn) {
  const samples = column.sample_values.filter((value) => value !== null && value !== "");
  if (samples.length === 0) return ["numeric", "integer", "float"].includes(column.inferred_type);
  return samples.every((value) => !Number.isNaN(Number(String(value).replace(",", "."))));
}

function isIntegerLikeColumn(column: PreviewColumn) {
  const samples = column.sample_values.filter((value) => value !== null && value !== "");
  if (samples.length === 0) return column.inferred_type === "integer";
  return samples.every((value) => {
    const parsed = Number(String(value).replace(",", "."));
    return Number.isFinite(parsed) && Number.isInteger(parsed);
  });
}

function isNumericFieldType(fieldType: FieldConfig["field_type"]) {
  return fieldType === "numeric" || fieldType === "integer" || fieldType === "float" || fieldType === "geo_latitude" || fieldType === "geo_longitude";
}

function inferUiType(column: PreviewColumn): FieldConfig["field_type"] {
  const normalizedName = column.normalized_name.toLowerCase();
  if (geoLatitudeHints.some((hint) => normalizedName.includes(hint))) {
    return "geo_latitude";
  }
  if (geoLongitudeHints.some((hint) => normalizedName.includes(hint))) {
    return "geo_longitude";
  }
  if (looksNumeric(column) || numericNameHints.some((hint) => column.normalized_name.includes(hint))) {
    return isIntegerLikeColumn(column) ? "integer" : "float";
  }
  return column.inferred_type;
}

function getUnitOptions(unitFamily?: string | null) {
  if (!unitFamily) return [];
  return UNIT_OPTIONS_BY_FAMILY[unitFamily] ?? [];
}

function uniqueSamples(column: PreviewColumn) {
  return Array.from(new Set(column.sample_values.filter((value) => value !== null && value !== "").map(String)));
}

function categoriesForField(preview: PreviewResponse | null, key: string, column?: PreviewColumn) {
  return categoryCountsForField(preview, key, undefined, column).map((item) => item.label);
}

function rawCategoryCountsForField(
  preview: PreviewResponse | null,
  key: string,
  column?: PreviewColumn,
) {
  const counts = new Map<string, number>();
  for (const row of preview?.normalized_dataset?.rows ?? []) {
    const value = row.values[key];
    if (value === null || value === undefined || value === "") continue;
    const label = String(value);
    counts.set(label, (counts.get(label) ?? 0) + 1);
  }

  if (counts.size) {
    return Array.from(counts.entries())
      .map(([label, value]) => ({ label, value }))
      .sort((left, right) => right.value - left.value || left.label.localeCompare(right.label));
  }

  return column ? uniqueSamples(column).map((label) => ({ label, value: 1 })) : [];
}

function categoryCountsForField(
  preview: PreviewResponse | null,
  key: string,
  fieldProfile?: FieldProfile,
  column?: PreviewColumn,
) {
  if (fieldProfile?.top_categories?.length) {
    return [...fieldProfile.top_categories].sort((left, right) => right.value - left.value || left.label.localeCompare(right.label));
  }

  const counts = new Map<string, number>();
  for (const row of preview?.normalized_dataset?.rows ?? []) {
    const value = row.values[key];
    if (value === null || value === undefined || value === "") continue;
    const label = String(value);
    counts.set(label, (counts.get(label) ?? 0) + 1);
  }

  if (counts.size) {
    return Array.from(counts.entries())
      .map(([label, value]) => ({ label, value }))
      .sort((left, right) => right.value - left.value || left.label.localeCompare(right.label));
  }

  const fromRows = Array.from(
    new Set(
      (preview?.normalized_dataset?.rows ?? [])
        .map((row) => row.values[key])
        .filter((value) => value !== null && value !== undefined && value !== "")
        .map(String),
    ),
  );
  if (fromRows.length) return fromRows.map((label) => ({ label, value: 1 }));
  return column ? uniqueSamples(column).map((label) => ({ label, value: 1 })) : [];
}

function buildOrdinalMap(samples: string[]) {
  if (!samples.length) return undefined;
  return Object.fromEntries(samples.map((value, index) => [value, Number(((index + 1) / samples.length).toFixed(2))]));
}

function mergeOrdinalMap(samples: string[], currentMap?: Record<string, number>) {
  const recommendedMap = buildOrdinalMap(samples) ?? {};
  if (!currentMap || !Object.keys(currentMap).length) {
    return recommendedMap;
  }
  return {
    ...recommendedMap,
    ...currentMap,
  };
}

function looksLikeLabelField(column: PreviewColumn) {
  const key = column.normalized_name.toLowerCase();
  return labelFieldHints.some((hint) => key.includes(hint));
}

function buildObjectLabel(
  row: { id: string; values: Record<string, unknown> } | undefined,
  fields: FieldConfig[],
) {
  if (!row) return "";
  const labelKeys = fields.filter((field) => field.use_in_label).map((field) => field.key);
  const values = labelKeys
    .map((key) => row.values[key])
    .filter((value) => value !== null && value !== undefined && String(value).trim() !== "")
    .map((value) => String(value).trim());
  if (values.length) return values.join(" · ");
  return String(row.values.name ?? row.values.title ?? row.values.object_name ?? `Объект ${row.id}`);
}

function buildObjectLabelFromValues(
  objectId: string,
  values: Record<string, unknown> | null | undefined,
  fields: FieldConfig[],
) {
  if (!values) return "";
  return buildObjectLabel({ id: objectId, values }, fields);
}

function humanizeFieldKey(key: string) {
  return key.replace(/_/g, " ").replace(/\b\w/g, (letter) => letter.toUpperCase());
}

function findGeoFieldKey(fields: FieldConfig[], role: "geo_latitude" | "geo_longitude") {
  return fields.find((field) => field.field_type === role && field.include_in_output)?.key ?? null;
}

function applyModeDefaultsToCriteria(criteria: CriterionConfig[], mode: AnalysisMode): CriterionConfig[] {
  return criteria.map((criterion) => ({
    ...criterion,
    direction: mode === "analog_search" ? "target" : "maximize",
  }));
}

function enforceRequiredScaling(fields: FieldConfig[]): FieldConfig[] {
  let changed = false;
  const next = fields.map((field) => {
    if (field.field_type === "categorical") {
      const nextOrdinalMap =
        field.ordinal_map && Object.keys(field.ordinal_map).length
          ? field.ordinal_map
          : { low: 0.25, medium: 0.5, high: 1 };
      if (field.encoding !== "ordinal" || !field.ordinal_map || !Object.keys(field.ordinal_map).length) {
        changed = true;
        return { ...field, encoding: "ordinal", ordinal_map: nextOrdinalMap };
      }
    }
    if (field.field_type === "geo_latitude" || field.field_type === "geo_longitude") {
      if (field.normalization !== "none") {
        changed = true;
        return { ...field, normalization: "none" };
      }
      return field;
    }
    if (isNumericFieldType(field.field_type) && (field.normalization === "none" || field.normalization === "")) {
      changed = true;
      return { ...field, normalization: "minmax" };
    }
    return field;
  });
  return changed ? next : fields;
}

function fieldDefaults(column: PreviewColumn): FieldConfig {
  const fieldType = inferUiType(column);
  const categories = uniqueSamples(column);
  return {
    key: column.normalized_name,
    field_type: fieldType,
    include_in_output: true,
    use_in_label: looksLikeLabelField(column),
    missing_strategy: "none",
    outlier_method: "none",
    outlier_threshold: 1.5,
    normalization: fieldType === "geo_latitude" || fieldType === "geo_longitude" ? "none" : (isNumericFieldType(fieldType) ? "minmax" : "none"),
    encoding: fieldType === "categorical" ? "ordinal" : "none",
    rounding_precision: fieldType === "float" ? 2 : null,
    datetime_format: fieldType === "datetime" ? "YYYY-MM-DD" : null,
    unit_family: null,
    target_unit: null,
    ordinal_map: fieldType === "categorical" ? buildOrdinalMap(categories) : undefined,
    binary_map: fieldType === "binary" ? { true: 1, false: 0, "1": 1, "0": 0 } : undefined,
  };
}

function buildResetFields(rawPreview: PreviewResponse | null, preview: PreviewResponse | null): FieldConfig[] {
  const sourceColumns = rawPreview?.columns ?? preview?.columns ?? [];
  return enforceRequiredScaling(sourceColumns.map((column) => fieldDefaults(column)));
}

function buildInitialFields(
  preview: PreviewResponse | null,
  profile: FieldProfile[],
): FieldConfig[] {
  const previewFields = (preview?.columns ?? []).map((column) => fieldDefaults(column));
  if (!profile.length) {
    return enforceRequiredScaling(previewFields);
  }

  const previewByKey = new Map(previewFields.map((field) => [field.key, field]));
  const nextFields = profile.map((field) => {
    const baseField = previewByKey.get(field.key) ?? {
      ...fieldDefaults({
        source_name: field.key,
        normalized_name: field.key,
        inferred_type: field.inferred_type,
        missing_count: field.missing_count,
        unique_count: field.unique_count,
        sample_values: field.sample_values,
      }),
      field_type: field.recommended_config.field_type,
    };
    const recommended = field.recommended_config;
    return {
      ...baseField,
      use_in_label: recommended.use_in_label ?? baseField.use_in_label,
      missing_strategy: recommended.missing_strategy ?? baseField.missing_strategy,
      outlier_method: recommended.outlier_method ?? baseField.outlier_method,
      outlier_threshold: recommended.outlier_threshold ?? baseField.outlier_threshold,
      normalization: recommended.normalization ?? baseField.normalization,
      encoding: recommended.encoding ?? baseField.encoding,
      ordinal_map: recommended.ordinal_map ?? baseField.ordinal_map,
      binary_map: recommended.binary_map ?? baseField.binary_map,
      rounding_precision: recommended.rounding_precision ?? baseField.rounding_precision,
      datetime_format: recommended.datetime_format ?? baseField.datetime_format,
      unit_family: recommended.unit_family ?? baseField.unit_family,
      target_unit: recommended.target_unit ?? baseField.target_unit,
    };
  });

  return enforceRequiredScaling(nextFields);
}

function mergeFieldsForSection(
  appliedFields: FieldConfig[],
  draftFields: FieldConfig[],
  section: PrepSectionId,
): FieldConfig[] {
  if (section === "preview") {
    return enforceRequiredScaling(draftFields);
  }

  const draftByKey = new Map(draftFields.map((field) => [field.key, field]));
  const merged = appliedFields.map((field) => {
    const draft = draftByKey.get(field.key);
    if (!draft) return field;

    if (section === "types") {
      return {
        ...field,
        field_type: draft.field_type,
        include_in_output: draft.include_in_output,
        use_in_label: draft.use_in_label,
        rounding_precision: draft.rounding_precision,
        datetime_format: draft.datetime_format,
        unit_family: draft.unit_family,
        target_unit: draft.target_unit,
      };
    }

    if (section === "encoding") {
      return {
        ...field,
        encoding: draft.encoding,
        ordinal_map: draft.ordinal_map,
        binary_map: draft.binary_map,
      };
    }

    if (section === "missing") {
      return {
        ...field,
        missing_strategy: draft.missing_strategy,
      };
    }

    if (section === "outliers") {
      return {
        ...field,
        outlier_method: draft.outlier_method,
        outlier_threshold: draft.outlier_threshold,
      };
    }

    if (section === "scaling") {
      return {
        ...field,
        normalization: draft.normalization,
      };
    }

    return field;
  });

  return enforceRequiredScaling(merged);
}

function numericSamples(column?: PreviewColumn) {
  if (!column) return [];
  return column.sample_values
    .map((value) => Number(String(value).replace(",", ".")))
    .filter((value) => Number.isFinite(value));
}

function numericValuesForField(preview: PreviewResponse | null, key: string) {
  return (preview?.normalized_dataset?.rows ?? [])
    .map((row) => Number(String(row.values[key] ?? "").replace(",", ".")))
    .filter((value) => Number.isFinite(value));
}

function histogramFromValues(values: number[], bins: number) {
  if (!values.length) return [];
  if (bins <= 1 || values.length === 1) {
    const value = values[0] ?? 0;
    return [{ label: String(value), value: values.length }];
  }

  const min = Math.min(...values);
  const max = Math.max(...values);
  if (min === max) {
    return [{ label: String(min), value: values.length }];
  }

  const counts = Array.from({ length: bins }, () => 0);
  for (const value of values) {
    const index = Math.min(bins - 1, Math.floor(((value - min) / (max - min)) * bins));
    counts[index] += 1;
  }

  const step = (max - min) / bins;
  return counts.map((count, index) => {
    const start = min + index * step;
    const end = index === bins - 1 ? max : start + step;
    return {
      label: `${start.toFixed(1)}–${end.toFixed(1)}`,
      value: count,
    };
  });
}

function boxplotStats(values: number[]) {
  if (!values.length) return null;
  const ordered = [...values].sort((left, right) => left - right);
  return {
    min: ordered[0],
    q1: percentile(ordered, 0.25),
    median: percentile(ordered, 0.5),
    q3: percentile(ordered, 0.75),
    max: ordered[ordered.length - 1],
  };
}

function percentile(values: number[], fraction: number) {
  if (!values.length) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  const index = (sorted.length - 1) * fraction;
  const lower = Math.floor(index);
  const upper = Math.ceil(index);
  if (lower === upper) return sorted[lower];
  return sorted[lower] + (sorted[upper] - sorted[lower]) * (index - lower);
}

function outlierRowsForField(preview: PreviewResponse | null, key: string, limit = 8) {
  const rows = preview?.normalized_dataset?.rows ?? [];
  if (!rows.length) return [];

  const numericRows = rows
    .map((row) => ({ rowId: row.id, value: Number(String(row.values[key] ?? "").replace(",", ".")) }))
    .filter((item) => Number.isFinite(item.value));

  if (numericRows.length < 4) return [];
  const values = numericRows.map((item) => item.value);
  const q1 = percentile(values, 0.25);
  const q3 = percentile(values, 0.75);
  const iqr = q3 - q1;
  const low = q1 - 1.5 * iqr;
  const high = q3 + 1.5 * iqr;

  return numericRows
    .filter((item) => item.value < low || item.value > high)
    .slice(0, limit)
    .map((item) => ({ id: item.rowId, value: item.value }));
}

function MiniDistribution({ column, fieldProfile }: { column?: PreviewColumn; fieldProfile?: FieldProfile }) {
  if (fieldProfile?.histogram.length) {
    const maxValue = Math.max(...fieldProfile.histogram.map((point) => point.value), 1);
    return (
      <div className="mini-chart" title="Histogram from preprocessing-service">
        {fieldProfile.histogram.map((point) => (
          <span key={point.label} title={`${point.label}: ${point.value}`} style={{ height: `${18 + (point.value / maxValue) * 54}px` }} />
        ))}
      </div>
    );
  }
  const values = numericSamples(column);
  if (values.length < 2) {
    return <div className="mini-chart empty">Недостаточно числовых примеров</div>;
  }
  const min = Math.min(...values);
  const max = Math.max(...values);
  return (
    <div className="mini-chart" title={`min: ${min}, max: ${max}`}>
      {values.map((value, index) => {
        const height = max === min ? 40 : 18 + ((value - min) / (max - min)) * 54;
        return <span key={`${value}-${index}`} style={{ height: `${height}px` }} />;
      })}
    </div>
  );
}

function DistributionChart({
  title,
  values,
  bins,
  mode,
  histogramData,
  boxplotData,
}: {
  title: string;
  values: number[];
  bins: number;
  mode: ChartMode;
  histogramData?: ChartPoint[];
  boxplotData?: { min: number; q1: number; median: number; q3: number; max: number } | null;
}) {
  const [expanded, setExpanded] = useState(false);
  const histogram = useMemo(
    () => (histogramData?.length ? histogramData : histogramFromValues(values, bins)),
    [histogramData, values, bins],
  );
  const stats = useMemo(() => boxplotData ?? boxplotStats(values), [boxplotData, values]);
  const outlierBounds = useMemo(() => {
    if (!stats) return null;
    const iqr = stats.q3 - stats.q1;
    return {
      low: stats.q1 - 1.5 * iqr,
      high: stats.q3 + 1.5 * iqr,
    };
  }, [stats]);

  if (!values.length) {
    return <p className="muted-note">Недостаточно данных для графика {title.toLowerCase()}.</p>;
  }

  const renderHistogram = (width: number, height: number) => {
    if (!histogram.length) {
      return null;
    }
    const maxValue = Math.max(...histogram.map((point) => point.value), 1);
    const barWidth = Math.max(10, Math.floor(width / Math.max(histogram.length, 1)) - 8);
    const labelStep = Math.max(1, Math.ceil(histogram.length / 12));
    const domainMin = stats?.min ?? Math.min(...values);
    const domainMax = stats?.max ?? Math.max(...values);
    const bounds =
      outlierBounds && domainMax > domainMin
        ? [
            { key: "low", value: outlierBounds.low },
            { key: "high", value: outlierBounds.high },
          ].filter((item) => item.value >= domainMin && item.value <= domainMax)
        : [];
    return (
      <svg width="100%" height={height + 52} viewBox={`0 0 ${width} ${height + 52}`} role="img" aria-label={title}>
        <rect x="0" y="0" width={width} height={height} fill="#f8fafc" stroke="#d7dde8" />
        <line x1="0" y1={height} x2={width} y2={height} stroke="#4b5563" strokeWidth="1" />
        <line x1="0" y1={Math.round(height * 0.75)} x2={width} y2={Math.round(height * 0.75)} stroke="#e1e8f0" strokeWidth="1" />
        <line x1="0" y1={Math.round(height * 0.5)} x2={width} y2={Math.round(height * 0.5)} stroke="#e1e8f0" strokeWidth="1" />
        <line x1="0" y1={Math.round(height * 0.25)} x2={width} y2={Math.round(height * 0.25)} stroke="#e1e8f0" strokeWidth="1" />
        {bounds.map((item) => {
          const x = ((item.value - domainMin) / (domainMax - domainMin)) * width;
          return (
            <g key={item.key}>
              <line x1={x} y1={6} x2={x} y2={height} stroke="#dc2626" strokeWidth="2" strokeDasharray="6 4" opacity="0.85" />
              <text x={x} y={14} fontSize="10" textAnchor="middle" fill="#dc2626">
                IQR
              </text>
            </g>
          );
        })}
        {histogram.map((point, index) => {
          const x = 4 + index * (barWidth + 8);
          const barHeight = Math.max(2, Math.round((point.value / maxValue) * (height - 12)));
          const y = height - barHeight;
          const shouldShowLabel = index % labelStep === 0 || index === histogram.length - 1;
          return (
            <g key={point.label}>
              <rect x={x} y={y} width={barWidth} height={barHeight} fill="#2f6b9a" opacity="0.9" rx="2" />
              {shouldShowLabel ? (
                <text
                  x={x + barWidth / 2}
                  y={height + 16}
                  fontSize="10"
                  textAnchor="middle"
                  fill="#334155"
                >
                  {point.label}
                </text>
              ) : null}
            </g>
          );
        })}
      </svg>
    );
  };

  const renderBoxplot = (width: number, height: number) => {
    if (!stats) {
      return null;
    }

    const padding = 56;
    const axisY = Math.round(height / 2);
    const usableWidth = Math.max(1, width - padding * 2);
    const span = stats.max - stats.min || 1;
    const scale = (value: number) => padding + ((value - stats.min) / span) * usableWidth;
    const minX = scale(stats.min);
    const q1X = scale(stats.q1);
    const medianX = scale(stats.median);
    const q3X = scale(stats.q3);
    const maxX = scale(stats.max);

    return (
      <svg width="100%" height={height + 52} viewBox={`0 0 ${width} ${height + 52}`} role="img" aria-label={title}>
        <rect x="0" y="0" width={width} height={height} fill="#f8fafc" stroke="#d7dde8" />
        <line x1={padding} y1={axisY} x2={width - padding} y2={axisY} stroke="#4b5563" strokeWidth="1" />
        <line x1={minX} y1={axisY} x2={q1X} y2={axisY} stroke="#2f6b9a" strokeWidth="3" />
        <line x1={q3X} y1={axisY} x2={maxX} y2={axisY} stroke="#2f6b9a" strokeWidth="3" />
        <rect x={q1X} y={axisY - 26} width={Math.max(4, q3X - q1X)} height={52} rx="5" fill="#2f6b9a" opacity="0.92" />
        <line x1={medianX} y1={axisY - 26} x2={medianX} y2={axisY + 26} stroke="#ffffff" strokeWidth="2" />
        <line x1={minX} y1={axisY - 12} x2={minX} y2={axisY + 12} stroke="#2f6b9a" strokeWidth="2" />
        <line x1={maxX} y1={axisY - 12} x2={maxX} y2={axisY + 12} stroke="#2f6b9a" strokeWidth="2" />
        {[
          { x: minX, label: `min ${stats.min.toFixed(1)}` },
          { x: q1X, label: `Q1 ${stats.q1.toFixed(1)}` },
          { x: medianX, label: `median ${stats.median.toFixed(1)}` },
          { x: q3X, label: `Q3 ${stats.q3.toFixed(1)}` },
          { x: maxX, label: `max ${stats.max.toFixed(1)}` },
        ].map((item) => (
          <text key={item.label} x={item.x} y={height + 16} fontSize="10" textAnchor="middle" fill="#334155">
            {item.label}
          </text>
        ))}
      </svg>
    );
  };

  const renderCurrentChart = (width: number, height: number) => (mode === "boxplot" ? renderBoxplot(width, height) : renderHistogram(width, height));

  return (
    <div className="chart-frame" title={title}>
      <div className="chart-toolbar">
        <strong>{title}</strong>
        <div className="chart-toggle"><span>{mode === "boxplot" ? "Ящик с усами" : "Гистограмма"}</span></div>
      </div>
      <button className="chart-canvas-trigger" onClick={() => setExpanded(true)} title="Открыть график в большом виде">
        {renderCurrentChart(980, 260)}
      </button>

      {expanded ? (
        <div className="modal-backdrop" role="presentation" onMouseDown={() => setExpanded(false)}>
          <section className="chart-modal" role="dialog" aria-modal="true" onMouseDown={(event) => event.stopPropagation()}>
            <div className="chart-modal-head">
              <h3>{title}</h3>
              <button className="modal-close" onClick={() => setExpanded(false)} aria-label="Закрыть">×</button>
            </div>
            <div className="chart-modal-body">
              {renderCurrentChart(1600, 440)}
            </div>
          </section>
        </div>
      ) : null}
    </div>
  );
}

function downloadBlob(filename: string, content: string, type: string) {
  const blob = new Blob([content], { type });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = filename;
  link.click();
  URL.revokeObjectURL(url);
}

function escapeCsvCell(value: unknown) {
  const text = String(value ?? "");
  if (text.includes('"') || text.includes(",") || text.includes("\n") || text.includes("\r")) {
    return `"${text.replace(/"/g, '""')}"`;
  }
  return text;
}

function formatPreviewCell(value: unknown) {
  if (value === null || value === undefined || value === "") {
    return "Пусто";
  }
  return String(value);
}

function Formula({ latex }: { latex: string }) {
  return (
    <span
      className="math-formula"
      aria-label={latex}
      dangerouslySetInnerHTML={{
        __html: katex.renderToString(latex, { throwOnError: false, strict: "ignore", displayMode: false }),
      }}
    />
  );
}

function CompactFormula({ children }: { children: ReactNode }) {
  return <span className="compact-formula">{children}</span>;
}

function HelpTip({ text }: { text: ReactNode }) {
  return (
    <span className="help-tip">
      <button type="button" className="help-tip-icon" aria-label="Пояснение">?</button>
      <span className="help-tip-popup" role="tooltip">{text}</span>
    </span>
  );
}

function LabelWithTip({ label, tip }: { label: string; tip: ReactNode }) {
  return (
    <span className="label-with-tip">
      <span>{label}</span>
      <HelpTip text={tip} />
    </span>
  );
}

function criteriaDefaults(
  fields: FieldConfig[],
  recommendedWeights?: Record<string, number>,
  analysisMode: AnalysisMode = "rating",
): CriterionConfig[] {
  const analyticFields = fields.filter(
    (field) =>
      !["text", "datetime", "geo_latitude", "geo_longitude"].includes(field.field_type)
      && field.include_in_output,
  );
  const fallbackWeight = analyticFields.length ? Number((1 / analyticFields.length).toFixed(4)) : 1;
  return analyticFields.map((field) => ({
    key: field.key,
    name: field.key.replace(/_/g, " "),
    weight: recommendedWeights?.[field.key] ?? fallbackWeight,
    type: field.field_type === "categorical" ? "categorical" : field.field_type === "binary" ? "binary" : "numeric",
    direction: analysisMode === "analog_search" ? "target" : "maximize",
    scale_map: field.ordinal_map ?? field.binary_map,
  }));
}

function normalizeCriteriaWeights(criteria: CriterionConfig[]): CriterionConfig[] {
  if (!criteria.length) return criteria;
  const total = criteria.reduce((sum, item) => sum + Number(item.weight || 0), 0);
  if (total <= 0) {
    const equalWeight = Number((1 / criteria.length).toFixed(4));
    const equalized = criteria.map((item) => ({ ...item, weight: equalWeight }));
    const drift = Number((1 - equalized.reduce((sum, item) => sum + item.weight, 0)).toFixed(4));
    if (equalized.length && drift) {
      equalized[0] = { ...equalized[0], weight: Number((equalized[0].weight + drift).toFixed(4)) };
    }
    return equalized;
  }
  const normalized = criteria.map((item) => ({
    ...item,
    weight: Number((Number(item.weight || 0) / total).toFixed(4)),
  }));
  const drift = Number((1 - normalized.reduce((sum, item) => sum + item.weight, 0)).toFixed(4));
  if (normalized.length && drift) {
    normalized[0] = { ...normalized[0], weight: Number((normalized[0].weight + drift).toFixed(4)) };
  }
  return normalized;
}

function syncCriteriaWithFields(
  fields: FieldConfig[],
  currentCriteria: CriterionConfig[],
  recommendedWeights?: Record<string, number>,
  analysisMode: AnalysisMode = "rating",
): CriterionConfig[] {
  const defaults = criteriaDefaults(fields, recommendedWeights, analysisMode);
  const currentByKey = new Map(currentCriteria.map((item) => [item.key, item]));
  const synced = defaults.map((item) => {
    const current = currentByKey.get(item.key);
    if (!current) return item;
    return {
      ...item,
      name: current.name || item.name,
      weight: current.weight,
      direction: analysisMode === "analog_search" ? "target" : current.direction,
      scale_map: current.scale_map ?? item.scale_map,
      target_value: current.target_value ?? item.target_value,
    };
  });
  return normalizeCriteriaWeights(synced);
}

const SUMMARY_LABELS: Record<string, string> = {
  objects_count: "Количество объектов",
  criteria_count: "Количество критериев",
  weights_sum: "Сумма весов",
  best_object_id: "Лучший объект",
  best_score: "Лучшая оценка",
  normalization_notes: "Нормализация",
  mode: "Режим анализа",
  target_object_id: "Целевой объект",
  confidence_score: "Доверие к расчету",
  confidence_notes: "Замечания к качеству",
  sensitivity: "Чувствительность",
  ranking_stability_note: "Устойчивость рейтинга",
  ranking_stability_scenarios: "Сценарии устойчивости",
  analog_groups: "Группы аналогов",
  dominance_pairs: "Доминирование объектов",
};

const SUMMARY_CARD_KEYS = [
  "objects_count",
  "criteria_count",
  "weights_sum",
  "best_object_id",
  "best_score",
  "mode",
  "target_object_id",
  "confidence_score",
];

const SUMMARY_HELP_TEXTS: Record<string, ReactNode> = {
  objects_count: "Количество объектов, включенных в расчет после фильтрации и предобработки.",
  criteria_count: "Количество критериев, участвующих в итоговой агрегированной оценке.",
  best_score: <><span>Максимальное значение интегрального показателя среди объектов.</span><Formula latex={"score = \\sum_{i=1}^{n} w_i \\cdot x_i^{norm}"} /></>,
  mode: "Режим расчета: ранжирование всех объектов либо поиск ближайших аналогов для целевого объекта.",
  target_object_id: "Идентификатор целевого объекта, относительно которого вычисляется мера близости аналогов.",
  confidence_score: (
    <>
      <span>Интегральная оценка надежности результата в диапазоне [0;1].</span>
      <Formula latex={"C = 1 - (aM + bO + cS)"} />
      <span>где M — доля пропусков, O — доля выбросов, S — чувствительность ранжирования.</span>
    </>
  ),
};

const DIRECTION_LABELS: Record<string, string> = {
  maximize: "Максимизация",
  minimize: "Минимизация",
  target: "Близость к целевому значению",
};

const MODE_LABELS: Record<string, string> = {
  rating: "Рейтинг объектов",
  analog_search: "Поиск аналогов",
};

const SERVICE_LABELS: Record<string, string> = {
  auth: "Авторизация",
  import: "Импорт",
  preprocessing: "Предобработка",
  analysis: "Анализ",
  storage: "Хранилище",
  pipeline: "Пайплайн",
  reports: "Отчеты",
  objects: "Объекты",
  system: "Система",
  other: "Прочее",
};

const STATUS_LABELS: Record<string, string> = {
  ok: "Работает",
  error: "Ошибка",
  active: "Активен",
  blocked: "Заблокирован",
  completed: "Завершен",
  warning: "Предупреждение",
  critical: "Критично",
  info: "Информация",
};

const QUALITY_TEXT: Record<string, string> = {
  good: "Хорошее",
  medium: "Среднее",
  poor: "Низкое",
  "Ready for analysis": "Готов к анализу",
  "Usable, but review recommendations": "Можно использовать, но нужно проверить рекомендации по преёбработке",
  "Risky, preprocessing is recommended": "Есть риски, рекомендуется предобработка",
  "Not ready for reliable analysis": "Не готов к надежному анализу",
  "No fields are currently suitable for comparative analysis.": "Нет полей, подходящих для сравнительного анализа.",
  "Dataset has fewer than 3 analytic fields, comparison may be unstable.": "В датасете меньше 3 аналитических полей, сравнение может быть неустойчивым.",
  "Some non-numeric fields look like identifiers or free text and are weak criteria.": "Некоторые нечисловые поля похожи на идентификаторы или свободный текст и слабо подходят как критерии.",
  "Dataset is very small; analog search and statistics may be fragile.": "Датасет очень мал, поиск аналогов и статистика могут быть неустойчивыми.",
};

const FIELD_TYPE_LABELS: Record<string, string> = {
  numeric: "Числовой",
  integer: "Целое Число",
  float: "Дробное Число",
  geo_latitude: "Гео широта",
  geo_longitude: "Гео долгота",
  categorical: "Категориальный",
  binary: "Бинарный",
  text: "Текстовый",
  datetime: "Дата/время",
};

const METHOD_LABELS: Record<string, string> = {
  none: "Не применять",
  ordinal: "Порядковый",
  binary_map: "Бинарная карта",
  median: "Заполнить медианой",
  mean: "Заполнить средним",
  mode: "Заполнить модой",
  drop_row: "Удалить строку",
  constant: "Задать константу",
  iqr_clip: "Ограничить по IQR",
  iqr_remove: "Удалить по IQR",
  zscore_clip: "Ограничить по z-score",
  zscore_remove: "Удалить по z-score",
  minmax: "min-max",
  zscore: "z-score",
  robust: "Устойчивая",
  log_minmax: "Логарифм + min-max",
};

function translateStatus(value: string) {
  return STATUS_LABELS[value] ?? value;
}

function fieldTypeLabel(value: string) {
  return FIELD_TYPE_LABELS[value] ?? value;
}

function methodLabel(value: string) {
  return METHOD_LABELS[value] ?? value;
}

function translateQualityText(value: string) {
  if (QUALITY_TEXT[value]) return QUALITY_TEXT[value];
  return value
    .replace(/^Dataset contains missing values in (\d+) fields\.$/, "Датасет содержит пропуски в $1 полях.")
    .replace(/^IQR-based profiling found possible outliers in (\d+) numeric fields\.$/, "Профилирование по IQR нашло возможные выбросы в $1 числовых полях.");
}

function labelForSummaryKey(key: string) {
  return SUMMARY_LABELS[key] ?? key.replace(/_/g, " ");
}

function summaryHelpText(key: string) {
  return SUMMARY_HELP_TEXTS[key];
}

function formatDirection(value: string) {
  return DIRECTION_LABELS[value] ?? value;
}

function formatAnalysisMode(value: unknown) {
  return MODE_LABELS[String(value)] ?? String(value ?? "-");
}

function translateAnalysisText(value: unknown) {
  return String(value ?? "-")
    .replace("Ranking is stable: the leader is ahead of the second object by", "Рейтинг устойчив: лидер опережает второй объект на")
    .replace("Ranking is moderately stable: the leader margin is", "Рейтинг умеренно устойчив: отрыв лидера составляет")
    .replace("Ranking is sensitive: the leader margin is only", "Рейтинг чувствителен: отрыв лидера составляет только")
    .replace("Dataset is sufficiently complete for the selected criteria.", "Датасет достаточно полный для выбранных критериев.")
    .replace("Very close analogs", "Очень близкие аналоги")
    .replace("Moderately close analogs", "Умеренно близкие аналоги")
    .replace("Weak analogs", "Слабые аналоги")
    .replace("Criterion has no spread and does not change ranking.", "Критерий не имеет разброса и не влияет на ранжирование.")
    .replace("Ranking is highly sensitive to this criterion.", "Рейтинг сильно чувствителен к этому критерию.")
    .replace("Criterion has a moderate influence on ranking.", "Критерий умеренно влияет на ранжирование.")
    .replace("Criterion has a low influence with current weights.", "При текущих весах критерий влияет слабо.");
}

function formatSummaryValue(key: string, value: unknown) {
  if (value === null || value === undefined) return "-";
  if (key === "mode") return formatAnalysisMode(value);
  if (key === "normalization_notes" || key === "confidence_notes") {
    return Array.isArray(value) && value.length ? value.map(translateAnalysisText).join("; ") : "Нет замечаний";
  }
  if (Array.isArray(value)) return `${value.length} записей`;
  if (typeof value === "number") return Number.isInteger(value) ? String(value) : value.toFixed(4);
  if (typeof value === "object") return "См. детальный раздел";
  return translateAnalysisText(value);
}

function summaryCardEntries(summary: Record<string, unknown>) {
  return SUMMARY_CARD_KEYS
    .filter((key) => key in summary)
    .map((key) => [key, summary[key]] as const);
}

function escapeHtml(value: unknown) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#039;");
}

type HistorySummary = {
  analysis_summary?: Record<string, unknown>;
  ranking?: Array<{ object_id: string; title: string; score: number; rank: number }>;
};

type HistoryParameters = {
  fields?: FieldConfig[];
  criteria?: CriterionConfig[];
  target_row_id?: string | null;
  geo_radius_km?: number | null;
  analysis_mode?: AnalysisMode;
  project_id?: number | null;
  scenario_title?: string | null;
};

const EMPTY_PREVIEW_RESPONSE: PreviewResponse = {
  filename: "",
  rows_total: 0,
  columns: [],
  preview_rows: [],
  warnings: [],
};

function normalizePipelineResult(result: PipelineResult | null | undefined): PipelineResult | null {
  if (!result) return null;
  return {
    import_preview: result.import_preview ?? EMPTY_PREVIEW_RESPONSE,
    preprocessing_summary: result.preprocessing_summary ?? {},
    analysis_summary: result.analysis_summary ?? {},
    history_id: result.history_id ?? null,
    ranking: Array.isArray(result.ranking) ? result.ranking : [],
  };
}

function compactPipelineResultForStorage(result: PipelineResult | null | undefined): PipelineResult | null {
  const normalized = normalizePipelineResult(result);
  if (!normalized) return null;
  return {
    ...normalized,
    import_preview: EMPTY_PREVIEW_RESPONSE,
    ranking: normalized.ranking.slice(0, 48).map((item) => ({
      ...item,
      contributions: item.contributions.slice(0, 12),
    })),
  };
}

type SavedWorkflowState = {
  activeStage?: StageId;
  datasetFileId?: number | null;
  rawPreview?: PreviewResponse | null;
  preview?: PreviewResponse | null;
  profile?: FieldProfile[];
  quality?: DatasetQualityReport | null;
  recommendedWeights?: Record<string, number>;
  recommendedCriteria?: CriterionConfig[];
  weightNotes?: string[];
  fields?: FieldConfig[];
  appliedFields?: FieldConfig[];
  criteria?: CriterionConfig[];
  analysisMode?: AnalysisMode;
  targetRowId?: string;
  geoRadiusKm?: number | null;
  result?: PipelineResult | null;
  activeProjectId?: number | null;
  historyProjectFilter?: number | "all";
  scenarioTitle?: string;
  sourceFilename?: string;
  preprocessingSection?: PrepSectionId | "selection";
  histogramBinsByField?: Record<string, number>;
  missingMatrixPreview?: Array<{ id: string; missing_count: number; missing_fields: string[] }>;
  missingRowsPreviewData?: Array<{ id: string; values: Record<string, unknown> }>;
  correlationMatrix?: Array<{ left_key: string; right_key: string; pearson: number; samples: number }>;
  lastHistoryId?: number | null;
  profileDetailLevel?: "summary" | "detailed";
};

const WORKFLOW_STORAGE_KEY = "comparison_workflow_state";

function readSavedWorkflow(): SavedWorkflowState {
  try {
    const stored = localStorage.getItem(WORKFLOW_STORAGE_KEY);
    return stored ? (JSON.parse(stored) as SavedWorkflowState) : {};
  } catch {
    return {};
  }
}

function parseHistorySummary(item?: ComparisonHistoryItem): HistorySummary {
  if (!item) return {};
  try {
    return JSON.parse(item.summary_json) as HistorySummary;
  } catch {
    return {};
  }
}

function parseHistoryParameters(item: ComparisonHistoryItem): HistoryParameters | null {
  try {
    return JSON.parse(item.parameters_json) as HistoryParameters;
  } catch {
    return null;
  }
}

function summaryNumber(summary: HistorySummary, key: string) {
  const value = summary.analysis_summary?.[key];
  return typeof value === "number" ? value : Number(value ?? 0);
}

function summaryList<T>(summary: Record<string, unknown>, key: string): T[] {
  const value = summary[key];
  return Array.isArray(value) ? (value as T[]) : [];
}

function buildHtmlReport(result: PipelineResult, criteria: CriterionConfig[]) {
  const objectLabelById = new Map(result.ranking.map((item) => [item.object_id, item.title || `№${item.object_id}`]));
  const maxScore = Math.max(...result.ranking.map((item) => item.score), 0.0001);
  const rankingBars = result.ranking
    .slice(0, 10)
    .map(
      (item) => `
        <div class="rank-bar">
          <span>${escapeHtml(item.title)}</span>
          <div><i style="width:${Math.max(4, (item.score / maxScore) * 100)}%"></i></div>
          <strong>${item.score.toFixed(4)}</strong>
        </div>`,
    )
    .join("");
  const rows = result.ranking
    .map(
      (item) => `
        <tr>
          <td>${item.rank}</td>
          <td>${escapeHtml(item.title)}</td>
          <td>${item.score.toFixed(4)}</td>
          <td>${escapeHtml(translateAnalysisText(item.explanation))}</td>
        </tr>`,
    )
    .join("");
  const criteriaRows = criteria
    .map(
      (item) => `
        <tr>
          <td>${escapeHtml(item.name)}</td>
          <td>${escapeHtml(item.key)}</td>
          <td>${item.weight}</td>
          <td>${escapeHtml(formatDirection(item.direction))}</td>
        </tr>`,
    )
    .join("");
  const summaryCards = summaryCardEntries(result.analysis_summary)
    .map(([key, value]) => `<div class="metric"><strong>${escapeHtml(labelForSummaryKey(key))}</strong><br />${escapeHtml(formatSummaryValue(key, value))}</div>`)
    .join("");
  const confidenceNotes = summaryList<string>(result.analysis_summary, "confidence_notes")
    .map((item) => `<li>${escapeHtml(translateAnalysisText(item))}</li>`)
    .join("");
  const sensitivityRows = summaryList<{ key: string; name: string; weight: number; normalized_range: number; sensitivity_index: number; note: string }>(
    result.analysis_summary,
    "sensitivity",
  )
    .map(
      (item) => `
        <tr>
          <td>${escapeHtml(item.name)}</td>
          <td>${item.weight.toFixed(4)}</td>
          <td>${item.normalized_range.toFixed(4)}</td>
          <td>${item.sensitivity_index.toFixed(4)}</td>
          <td>${escapeHtml(translateAnalysisText(item.note))}</td>
        </tr>`,
    )
    .join("");
  const analogRows = summaryList<{ label: string; object_ids: string[] }>(result.analysis_summary, "analog_groups")
    .map((group) => `<tr><td>${escapeHtml(translateAnalysisText(group.label))}</td><td>${group.object_ids.map((id) => escapeHtml(objectLabelById.get(id) || `№${id}`)).join(", ")}</td></tr>`)
    .join("");
  const dominanceRows = summaryList<{ dominant_object_id: string; dominated_object_id: string; criteria_count: number }>(
    result.analysis_summary,
    "dominance_pairs",
  )
    .map((item) => `<tr><td>${escapeHtml(objectLabelById.get(item.dominant_object_id) || `№${item.dominant_object_id}`)}</td><td>${escapeHtml(objectLabelById.get(item.dominated_object_id) || `№${item.dominated_object_id}`)}</td><td>${item.criteria_count}</td></tr>`)
    .join("");
  return `<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8" />
  <title>Отчет сравнительного анализа</title>
  <style>
    body { font-family: Segoe UI, sans-serif; padding: 32px; color: #1c2430; }
    h1 { font-size: 32px; }
    table { width: 100%; border-collapse: collapse; margin: 20px 0; }
    th, td { border-bottom: 1px solid #d7dde8; padding: 10px; text-align: left; vertical-align: top; }
    th { background: #f2f5f9; }
    .metric { display: inline-block; margin: 8px 16px 8px 0; padding: 12px 16px; background: #f6f8fb; border-radius: 12px; }
    .note { padding: 14px 16px; border-radius: 12px; background: #eef7ff; }
    .report-intro { margin: 14px 0 22px; line-height: 1.55; }
    .rank-bars { display: grid; gap: 10px; margin: 18px 0 24px; }
    .rank-bar { display: grid; grid-template-columns: minmax(180px, 1fr) 3fr auto; gap: 10px; align-items: center; }
    .rank-bar div { height: 14px; border-radius: 999px; background: #e9eff7; overflow: hidden; }
    .rank-bar i { display: block; height: 100%; border-radius: 999px; background: linear-gradient(90deg, #0078d4, #16a0f7); }
    .rank-bar span { font-size: 13px; }
    .rank-bar strong { font-size: 12px; color: #334155; }
  </style>
</head>
<body>
  <h1>Отчет сравнительного анализа</h1>
  <p>Сформировано: ${new Date().toLocaleString("ru-RU")}</p>
  <p class="report-intro">Отчет показывает итоговый рейтинг объектов, вклад критериев, устойчивость результата и ключевые аналитические выводы на русском языке. Графики ниже помогают визуально сравнить лидеров по итоговому баллу.</p>
  <h2>Сводка</h2>
  ${summaryCards}
  <p class="note">${escapeHtml(translateAnalysisText(result.analysis_summary.ranking_stability_note ?? "Нет данных об устойчивости рейтинга."))}</p>
  ${confidenceNotes ? `<h3>Замечания к расчету</h3><ul>${confidenceNotes}</ul>` : ""}
  <h2>Критерии</h2>
  <table><thead><tr><th>Название</th><th>Поле датасета</th><th>Вес</th><th>Направление</th></tr></thead><tbody>${criteriaRows}</tbody></table>
  ${sensitivityRows ? `<h2>Чувствительность критериев</h2><table><thead><tr><th>Критерий</th><th>Вес</th><th>Разброс</th><th>Индекс</th><th>Пояснение</th></tr></thead><tbody>${sensitivityRows}</tbody></table>` : ""}
  ${analogRows ? `<h2>Группы аналогов</h2><table><thead><tr><th>Группа</th><th>Объекты</th></tr></thead><tbody>${analogRows}</tbody></table>` : ""}
  ${dominanceRows ? `<h2>Доминирование объектов</h2><table><thead><tr><th>Доминирующий объект</th><th>Уступающий объект</th><th>Критериев</th></tr></thead><tbody>${dominanceRows}</tbody></table>` : ""}
  <h2>График итоговых оценок</h2>
  <div class="rank-bars">${rankingBars}</div>
  <h2>Рейтинг</h2>
  <table><thead><tr><th>Место</th><th>Объект</th><th>Оценка</th><th>Объяснение</th></tr></thead><tbody>${rows}</tbody></table>
</body>
</html>`;
}

type RankedItem = PipelineResult["ranking"][number];

function chartValue(item: RankedItem, mode: AnalysisMode) {
  if (mode === "analog_search" && item.similarity_to_target !== null && item.similarity_to_target !== undefined) {
    return item.similarity_to_target;
  }
  return item.score;
}

function compactNumber(value: number) {
  return Number.isFinite(value) ? value.toFixed(4).replace(/0+$/, "").replace(/\.$/, "") : "0";
}

function objectLabelFromResult(result: PipelineResult, objectId: string) {
  return result.ranking.find((item) => item.object_id === objectId)?.title || `Объект ${objectId}`;
}

function objectValueLabel(value: unknown) {
  if (value === null || value === undefined || value === "") {
    return "Пусто";
  }
  if (typeof value === "number") {
    return Number.isFinite(value) ? compactNumber(value) : "Пусто";
  }
  if (typeof value === "boolean") {
    return value ? "Да" : "Нет";
  }
  return String(value);
}

function buildSortedDatasetCsv(result: PipelineResult, criteria: CriterionConfig[], mode: AnalysisMode) {
  const headers = [
    "rank",
    "object_id",
    "title",
    "score",
    "similarity_to_target",
    ...criteria.flatMap((criterion) => [
      `${criterion.key}_raw`,
      `${criterion.key}_normalized`,
      `${criterion.key}_contribution`,
    ]),
  ];

  const rows = result.ranking.map((item) => {
    const contributionByKey = new Map(item.contributions.map((contribution) => [contribution.key, contribution]));
    const scoreValue = mode === "analog_search" && item.similarity_to_target !== null && item.similarity_to_target !== undefined
      ? item.similarity_to_target
      : item.score;
    return [
      item.rank,
      item.object_id,
      item.title,
      Number(scoreValue).toFixed(6),
      item.similarity_to_target === null || item.similarity_to_target === undefined ? "" : Number(item.similarity_to_target).toFixed(6),
      ...criteria.flatMap((criterion) => {
        const contribution = contributionByKey.get(criterion.key);
        if (!contribution) {
          return ["", "", ""];
        }
        return [
          objectValueLabel(contribution.raw_value),
          Number(contribution.normalized_value).toFixed(6),
          Number(contribution.contribution).toFixed(6),
        ];
      }),
    ];
  });

  const lines = [headers, ...rows].map((line) => line.map(escapeCsvCell).join(","));
  return lines.join("\n");
}

function ResultBarChart({
  result,
  objectLabelMap,
  mode,
  selectedId,
  onSelect,
  currentPage,
  onPageChange,
}: {
  result: PipelineResult;
  objectLabelMap: Map<string, string>;
  mode: AnalysisMode;
  selectedId: string;
  onSelect: (objectId: string) => void;
  currentPage: number;
  onPageChange: (page: number) => void;
}) {
  const pageSize = 8;
  const totalPages = Math.max(1, Math.ceil(result.ranking.length / pageSize));
  const safePage = Math.min(Math.max(1, currentPage), totalPages);
  const pageStart = (safePage - 1) * pageSize;
  const rows = result.ranking.slice(pageStart, pageStart + pageSize);
  const maxValue = Math.max(...rows.map((item) => chartValue(item, mode)), 0.0001);
  return (
    <div className="analytics-card wide-chart">
      <div className="chart-head">
        <div>
          <span className="section-kicker">Bar chart</span>
          <h3>{mode === "analog_search" ? "Близость Аналогов" : "Итоговые Оценки"}</h3>
          <p className="chart-subtitle">
            Показаны Объекты {pageStart + 1}-{Math.min(pageStart + rows.length, result.ranking.length)} из {result.ranking.length}
          </p>
        </div>
        <small>Нажмите на столбец, чтобы посмотреть объект ниже</small>
      </div>
      <div className="result-bars">
        {rows.map((item) => {
          const value = chartValue(item, mode);
          const width = Math.max(3, (value / maxValue) * 100);
          const objectLabel = objectLabelMap.get(String(item.object_id)) || objectLabelFromResult(result, item.object_id);
          return (
            <button
              className={`result-bar ${selectedId === item.object_id ? "active" : ""}`}
              key={item.object_id}
              onClick={() => onSelect(item.object_id)}
              title={`${objectLabel}: ${compactNumber(value)}`}
            >
              <span className="result-bar-rank">#{item.rank}</span>
              <span className="result-bar-label">{objectLabel}</span>
              <span className="result-bar-track">
                <span style={{ width: `${width}%` }} />
              </span>
              <strong>{compactNumber(value)}</strong>
            </button>
          );
        })}
      </div>
      {totalPages > 1 ? (
        <div className="chart-pagination">
          <div className="chart-page-info">Страница {safePage} из {totalPages}</div>
          <div className="chart-pagination-controls">
            <button className="ghost-button" onClick={() => onPageChange(safePage - 1)} disabled={safePage <= 1}>
              Назад
            </button>
            <div className="page-pills">
              {Array.from({ length: totalPages }, (_, index) => index + 1)
                .slice(Math.max(0, safePage - 3), Math.min(totalPages, safePage + 2))
                .map((page) => (
                  <button
                    key={page}
                    className={`page-pill ${page === safePage ? "active" : ""}`}
                    onClick={() => onPageChange(page)}
                  >
                    {page}
                  </button>
                ))}
            </div>
            <button className="ghost-button" onClick={() => onPageChange(safePage + 1)} disabled={safePage >= totalPages}>
              Далее
            </button>
          </div>
        </div>
      ) : null}
    </div>
  );
}

function RadarComparisonChart({
  item,
  baseline,
}: {
  item?: RankedItem;
  baseline?: RankedItem;
}) {
  if (!item) {
    return <p className="muted-note">Выберите объект, чтобы увидеть профиль признаков на радаре.</p>;
  }

  const contributions = item.contributions.slice(0, 8);
  if (!contributions.length) {
    return <p className="muted-note">Для выбранного объекта пока нет данных для radar chart.</p>;
  }

  const baselineByKey = new Map((baseline?.contributions ?? []).map((contribution) => [contribution.key, contribution]));
  const centerX = 130;
  const centerY = 126;
  const radius = 82;

  const polygonPoints = contributions.map((contribution, index) => {
    const angle = (-Math.PI / 2) + (index / contributions.length) * Math.PI * 2;
    const valueRadius = radius * Math.max(0, Math.min(1, contribution.normalized_value));
    return {
      x: centerX + Math.cos(angle) * valueRadius,
      y: centerY + Math.sin(angle) * valueRadius,
      labelX: centerX + Math.cos(angle) * (radius + 22),
      labelY: centerY + Math.sin(angle) * (radius + 22),
      axisX: centerX + Math.cos(angle) * radius,
      axisY: centerY + Math.sin(angle) * radius,
    };
  });

  const baselinePoints = contributions.map((contribution, index) => {
    const baselineContribution = baselineByKey.get(contribution.key);
    const baselineValue = baselineContribution?.normalized_value ?? 0;
    const angle = (-Math.PI / 2) + (index / contributions.length) * Math.PI * 2;
    const valueRadius = radius * Math.max(0, Math.min(1, baselineValue));
    return `${centerX + Math.cos(angle) * valueRadius},${centerY + Math.sin(angle) * valueRadius}`;
  });

  const selectedPolygon = polygonPoints.map((point) => `${point.x},${point.y}`).join(" ");
  const baselinePolygon = baselinePoints.join(" ");

  return (
    <div className="analytics-card">
      <div className="chart-head">
        <div>
          <span className="section-kicker">Radar chart</span>
          <h3>Профиль критериев</h3>
          <p className="chart-subtitle">Сравнение выбранного объекта с лидером текущего рейтинга.</p>
        </div>
      </div>
      <svg className="radar-chart" viewBox="0 0 260 260" role="img" aria-label="Радар критериев">
        {[0.25, 0.5, 0.75, 1].map((level) => (
          <circle
            key={level}
            cx={centerX}
            cy={centerY}
            r={radius * level}
            fill="none"
            stroke="#d7dde8"
            strokeDasharray={level === 1 ? "0" : "4 4"}
          />
        ))}
        {polygonPoints.map((point, index) => (
          <g key={contributions[index].key}>
            <line x1={centerX} y1={centerY} x2={point.axisX} y2={point.axisY} stroke="#d7dde8" />
            <text
              x={point.labelX}
              y={point.labelY}
              textAnchor={point.labelX < centerX - 12 ? "end" : point.labelX > centerX + 12 ? "start" : "middle"}
              fontSize="9.5"
              fill="#334155"
            >
              {contributions[index].name}
            </text>
          </g>
        ))}
        {baseline ? <polygon points={baselinePolygon} fill="rgba(15, 23, 42, 0.08)" stroke="#64748b" strokeWidth="2" /> : null}
        <polygon points={selectedPolygon} fill="rgba(14, 116, 144, 0.22)" stroke="#0f766e" strokeWidth="2.5" />
      </svg>
      <div className="chart-legend">
        <span><i style={{ background: "#0f766e" }} />Выбранный объект</span>
        {baseline ? <span><i style={{ background: "#64748b" }} />Лидер рейтинга</span> : null}
      </div>
    </div>
  );
}

function ContributionWaterfallChart({
  item,
  mode,
}: {
  item?: RankedItem;
  mode: AnalysisMode;
}) {
  if (!item) {
    return <p className="muted-note">Выберите объект, чтобы увидеть структуру итоговой оценки.</p>;
  }

  const contributions = item.contributions.slice(0, 10);
  if (!contributions.length) {
    return <p className="muted-note">Для выбранного объекта пока нет вкладов критериев.</p>;
  }

  const steps = contributions.map((contribution) => ({
    ...contribution,
    start: 0,
    end: 0,
  }));
  let running = 0;
  for (const step of steps) {
    step.start = running;
    running += step.contribution;
    step.end = running;
  }

  const minValue = Math.min(0, ...steps.map((step) => Math.min(step.start, step.end)));
  const maxValue = Math.max(
    mode === "analog_search" && item.similarity_to_target !== null && item.similarity_to_target !== undefined
      ? item.similarity_to_target
      : item.score,
    ...steps.map((step) => Math.max(step.start, step.end)),
    0.0001,
  );
  const range = Math.max(maxValue - minValue, 0.0001);
  const zeroOffset = ((0 - minValue) / range) * 100;

  return (
    <div className="analytics-card">
      <div className="chart-head">
        <div>
          <span className="section-kicker">Waterfall</span>
          <h3>Структура итоговой оценки</h3>
          <p className="chart-subtitle">Каждая полоса показывает вклад критерия в итоговый результат объекта.</p>
        </div>
      </div>
      <div className="waterfall-list">
        {steps.map((contribution) => {
          const left = ((Math.min(contribution.start, contribution.end) - minValue) / range) * 100;
          const width = Math.max(2, (Math.abs(contribution.end - contribution.start) / range) * 100);
          const isPositive = contribution.contribution >= 0;
          return (
            <div className="waterfall-row" key={contribution.key}>
              <div className="waterfall-meta">
                <strong>{contribution.name}</strong>
                <span>{objectValueLabel(contribution.raw_value)}</span>
              </div>
              <div className="waterfall-track">
                <span className="waterfall-zero" style={{ left: `${zeroOffset}%` }} />
                <span
                  className={`waterfall-bar ${isPositive ? "positive" : "negative"}`}
                  style={{ left: `${left}%`, width: `${width}%` }}
                />
              </div>
              <strong className={`waterfall-value ${isPositive ? "positive" : "negative"}`}>
                {contribution.contribution >= 0 ? "+" : ""}
                {contribution.contribution.toFixed(3)}
              </strong>
            </div>
          );
        })}
      </div>
    </div>
  );
}

function ObjectDetailsPanel({
  item,
  objectLabelMap,
  result,
  mode,
  rawValues,
}: {
  item?: RankedItem;
  objectLabelMap: Map<string, string>;
  result: PipelineResult;
  mode: AnalysisMode;
  rawValues?: Record<string, unknown> | null;
}) {
  if (!item) {
    return <p className="muted-note">Выберите объект на Bar Chart, чтобы увидеть подробности.</p>;
  }

  const objectLabel = objectLabelMap.get(String(item.object_id)) || objectLabelFromResult(result, item.object_id);

  return (
    <section className="analytics-card object-details-panel">
      <div className="object-modal-head">
        <div>
          <span className="section-kicker">Выбранный объект</span>
          <h3>{objectLabel}</h3>
          <p>{objectLabel}</p>
        </div>
      </div>

      <div className="object-meta-grid">
        <div className="metric mini">
          <span>Место</span>
          <strong>#{item.rank}</strong>
        </div>
        <div className="metric mini">
          <span>
            <LabelWithTip
              label={mode === "analog_search" ? "Близость" : "Оценка"}
              tip={
                mode === "analog_search"
                  ? "Мера сходства объекта с целевым по совокупности критериев и весов (диапазон [0;1], где 1 — максимальная близость)."
                  : <><span>Интегральная оценка объекта:</span><Formula latex={"score = \\sum_{i=1}^{n} w_i \\cdot x_i^{norm}"} /></>
              }
            />
          </span>
          <strong>
            {mode === "analog_search" && item.similarity_to_target !== null && item.similarity_to_target !== undefined
              ? item.similarity_to_target.toFixed(4)
              : item.score.toFixed(4)}
          </strong>
        </div>
      </div>

      <div className="object-contributions-card">
        <span className="section-kicker">Критерии и вклады</span>
        <div className="table-wrap object-contributions-table">
          <table>
            <thead>
              <tr>
                <th><LabelWithTip label="Критерий" tip="Показатель, используемый для сравнительного анализа объектов." /></th>
                <th><LabelWithTip label="Исходное" tip="Исходное значение показателя для выбранного объекта до преобразований." /></th>
                <th><LabelWithTip label="Преобразованное" tip="Значение показателя после предобработки: обработки единиц, пропусков, выбросов, кодирования и масштабирования." /></th>
                <th><LabelWithTip label="Нормализованное" tip="Преобразованное значение показателя в единой шкале для сопоставимости критериев." /></th>
                <th><LabelWithTip label="Вклад" tip={<><span>Компонент итогового балла по критерию:</span><Formula latex={"contribution_i = w_i \\cdot x_i^{norm}"} /></>} /></th>
              </tr>
            </thead>
            <tbody>
              {item.contributions.map((contribution) => (
                <tr key={contribution.key}>
                  <td><strong>{contribution.name}</strong></td>
                  <td title={objectValueLabel(rawValues?.[contribution.key] ?? contribution.raw_value)}>{objectValueLabel(rawValues?.[contribution.key] ?? contribution.raw_value)}</td>
                  <td title={objectValueLabel(contribution.transformed_value ?? contribution.raw_value)}>{objectValueLabel(contribution.transformed_value ?? contribution.raw_value)}</td>
                  <td>{compactNumber(contribution.normalized_value)}</td>
                  <td>{contribution.contribution.toFixed(3)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </section>
  );
}

export function App() {
  const savedWorkflow = useMemo(readSavedWorkflow, []);
  const [activeStage, setActiveStage] = useState<StageId>(savedWorkflow.activeStage ?? "data");
  const [token, setToken] = useState(() => localStorage.getItem("access_token") ?? "");
  const [user, setUser] = useState<AuthUser | null>(() => {
    const stored = localStorage.getItem("auth_user");
    return stored ? JSON.parse(stored) as AuthUser : null;
  });
  const [authMode, setAuthMode] = useState<"login" | "register">("login");
  const [authModalOpen, setAuthModalOpen] = useState(false);
  const [authError, setAuthError] = useState<string | null>(null);
  const [reportPreviewOpen, setReportPreviewOpen] = useState(false);
  const [reportPreviewHtml, setReportPreviewHtml] = useState("");
  const [userMenuOpen, setUserMenuOpen] = useState(false);
  const [authEmail, setAuthEmail] = useState("admin@example.com");
  const [authPassword, setAuthPassword] = useState("admin12345");
  const [authName, setAuthName] = useState("Пользователь");
  const [history, setHistory] = useState<ComparisonHistoryItem[]>([]);
  const [projects, setProjects] = useState<ProjectItem[]>([]);
  const [activeProjectId, setActiveProjectId] = useState<number | null>(savedWorkflow.activeProjectId ?? null);
  const [historyProjectFilter, setHistoryProjectFilter] = useState<number | "all">(savedWorkflow.historyProjectFilter ?? "all");
  const [historyCompareIds, setHistoryCompareIds] = useState<number[]>([]);
  const [newProjectName, setNewProjectName] = useState("Новый проект сравнения");
  const [scenarioTitle, setScenarioTitle] = useState(savedWorkflow.scenarioTitle ?? "Сценарий сравнительного анализа");
  const [systemDashboard, setSystemDashboard] = useState<SystemDashboard | null>(null);
  const [adminStats, setAdminStats] = useState<AdminStats | null>(null);
  const [adminUsers, setAdminUsers] = useState<AuthUser[]>([]);
  const [file, setFile] = useState<File | null>(null);
  const [datasetFileId, setDatasetFileId] = useState<number | null>(savedWorkflow.datasetFileId ?? null);
  const [sourceFilename, setSourceFilename] = useState(savedWorkflow.sourceFilename ?? "");
  const [rawPreview, setRawPreview] = useState<PreviewResponse | null>(savedWorkflow.rawPreview ?? savedWorkflow.preview ?? null);
  const [preview, setPreview] = useState<PreviewResponse | null>(savedWorkflow.preview ?? null);
  const [profile, setProfile] = useState<FieldProfile[]>(savedWorkflow.profile ?? []);
  const [quality, setQuality] = useState<DatasetQualityReport | null>(savedWorkflow.quality ?? null);
  const [recommendedWeights, setRecommendedWeights] = useState<Record<string, number>>(savedWorkflow.recommendedWeights ?? {});
  const [weightNotes, setWeightNotes] = useState<string[]>(savedWorkflow.weightNotes ?? []);
  const [missingMatrixPreview, setMissingMatrixPreview] = useState<Array<{ id: string; missing_count: number; missing_fields: string[] }>>(
    savedWorkflow.missingMatrixPreview ?? [],
  );
  const [missingRowsPreviewData, setMissingRowsPreviewData] = useState<Array<{ id: string; values: Record<string, unknown> }>>(
    savedWorkflow.missingRowsPreviewData ?? [],
  );
  const [correlationMatrix, setCorrelationMatrix] = useState<Array<{ left_key: string; right_key: string; pearson: number; samples: number }>>(
    savedWorkflow.correlationMatrix ?? [],
  );
  const [fields, setFields] = useState<FieldConfig[]>(savedWorkflow.fields ?? []);
  const [appliedFields, setAppliedFields] = useState<FieldConfig[]>(savedWorkflow.appliedFields ?? savedWorkflow.fields ?? []);
  const [criteria, setCriteria] = useState<CriterionConfig[]>(savedWorkflow.criteria ?? []);
  const [analysisMode, setAnalysisMode] = useState<AnalysisMode>(savedWorkflow.analysisMode ?? "analog_search");
  const [targetRowId, setTargetRowId] = useState(savedWorkflow.targetRowId ?? "");
  const [geoRadiusKm, setGeoRadiusKm] = useState<number | null>(savedWorkflow.geoRadiusKm ?? null);
  const [result, setResult] = useState<PipelineResult | null>(() => normalizePipelineResult(savedWorkflow.result ?? null));
  const [selectedResultId, setSelectedResultId] = useState(savedWorkflow.result?.ranking?.[0]?.object_id ?? "");
  const [rawResultValuesById, setRawResultValuesById] = useState<Record<string, Record<string, unknown>>>({});
  const [resultsPage, setResultsPage] = useState(1);
  const [lastHistoryId, setLastHistoryId] = useState<number | null>(savedWorkflow.lastHistoryId ?? null);
  const [preprocessingSection, setPreprocessingSection] = useState<PrepSectionId>(savedWorkflow.preprocessingSection === "selection" ? "types" : savedWorkflow.preprocessingSection ?? "types");
  const [histogramBinsByField, setHistogramBinsByField] = useState<Record<string, number>>(savedWorkflow.histogramBinsByField ?? {});
  const [histogramDraftBinsByField, setHistogramDraftBinsByField] = useState<Record<string, number>>(savedWorkflow.histogramBinsByField ?? {});
  const [chartModeByField, setChartModeByField] = useState<Record<string, ChartMode>>({});
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [applyingSection, setApplyingSection] = useState<PrepSectionId | null>(null);
  const [profileDetailLevel, setProfileDetailLevel] = useState<"summary" | "detailed">(savedWorkflow.profileDetailLevel ?? "summary");
  const [profileDetailsLoading, setProfileDetailsLoading] = useState(false);

  const totalWeight = useMemo(() => criteria.reduce((sum, item) => sum + Number(item.weight || 0), 0), [criteria]);
  const activePreprocessingFields = useMemo(
    () => fields.filter((field) => field.include_in_output),
    [fields],
  );
  const visiblePreviewColumns = useMemo(() => {
    const allowed = new Set(activePreprocessingFields.map((field) => field.key));
    return (preview?.columns ?? []).filter((column) => allowed.has(column.normalized_name));
  }, [activePreprocessingFields, preview]);
  const completedStages = {
    data: Boolean(preview),
    preprocessing: fields.length > 0,
    criteria: criteria.length > 0,
    results: Boolean(result),
    projects: projects.length > 0,
    history: history.length > 0,
    admin: Boolean(adminStats),
  };
  const filteredHistory = useMemo(
    () =>
      historyProjectFilter === "all"
        ? history
        : history.filter((item) => item.project_id === historyProjectFilter),
    [history, historyProjectFilter],
  );
  const comparedHistory = historyCompareIds
    .map((id) => history.find((item) => item.id === id))
    .filter((item): item is ComparisonHistoryItem => Boolean(item));
  const selectedResult = useMemo(
    () => result?.ranking.find((item) => item.object_id === selectedResultId) ?? result?.ranking[0],
    [result, selectedResultId],
  );
  const activeProjectLatestHistory = useMemo(() => {
    if (!activeProjectId) return null;
    return [...history]
      .filter((item) => item.project_id === activeProjectId)
      .sort((left, right) => Date.parse(right.created_at) - Date.parse(left.created_at))[0] ?? null;
  }, [history, activeProjectId]);
  const missingRowsPreview = useMemo(() => {
    if (missingRowsPreviewData.length) return missingRowsPreviewData;
    const rows = preview?.normalized_dataset?.rows ?? [];
    if (!rows.length || !missingMatrixPreview.length) return [];
    const ids = new Set(missingMatrixPreview.map((item) => item.id));
    return rows.filter((row) => ids.has(row.id)).slice(0, 14);
  }, [preview, missingMatrixPreview, missingRowsPreviewData]);
  const transformedPreviewRows = useMemo(
    () => (preview?.normalized_dataset?.rows ?? []).slice(0, 40),
    [preview],
  );
  const objectLabelMap = useMemo(() => {
    const rows = preview?.normalized_dataset?.rows ?? [];
    return new Map(rows.map((row) => [String(row.id), buildObjectLabel(row, appliedFields)]));
  }, [preview, appliedFields]);
  const displayResult = useMemo(() => {
    if (!result) return null;
    return {
      ...result,
      ranking: result.ranking.map((item) => {
        const rawLabel = buildObjectLabelFromValues(item.object_id, rawResultValuesById[item.object_id], appliedFields);
        return {
          ...item,
          title: rawLabel || objectLabelMap.get(String(item.object_id)) || item.title || `Объект ${item.object_id}`,
        };
      }),
    };
  }, [result, rawResultValuesById, appliedFields, objectLabelMap]);
  const selectedDisplayResult = useMemo(
    () => displayResult?.ranking.find((item) => item.object_id === selectedResultId) ?? displayResult?.ranking[0],
    [displayResult, selectedResultId],
  );
  const encodingPreview = rawPreview ?? preview;
  const geoLatitudeKey = useMemo(() => findGeoFieldKey(fields, "geo_latitude"), [fields]);
  const geoLongitudeKey = useMemo(() => findGeoFieldKey(fields, "geo_longitude"), [fields]);
  const geoSearchAvailable = Boolean(geoLatitudeKey && geoLongitudeKey);
  const selectedTargetRow = useMemo(
    () => (preview?.normalized_dataset?.rows ?? []).find((row) => row.id === targetRowId) ?? null,
    [preview, targetRowId],
  );
  const selectedTargetPreviewItems = useMemo(() => {
    if (!selectedTargetRow) return [];

    const selectedKeys = new Set<string>();
    const orderedKeys: string[] = [];
    const pushKey = (key: string | null | undefined) => {
      if (!key || selectedKeys.has(key)) return;
      const value = selectedTargetRow.values[key];
      if (value === null || value === undefined || String(value).trim() === "") return;
      selectedKeys.add(key);
      orderedKeys.push(key);
    };

    fields
      .filter((field) => field.use_in_label && field.include_in_output)
      .forEach((field) => pushKey(field.key));
    criteria.forEach((criterion) => pushKey(criterion.key));
    pushKey(geoLatitudeKey);
    pushKey(geoLongitudeKey);
    fields
      .filter((field) => field.include_in_output)
      .forEach((field) => pushKey(field.key));

    return orderedKeys.slice(0, 8).map((key) => ({
      key,
      label: humanizeFieldKey(key),
      value: objectValueLabel(selectedTargetRow.values[key]),
    }));
  }, [selectedTargetRow, fields, criteria, geoLatitudeKey, geoLongitudeKey]);
  const visibleOutlierFieldKeys = useMemo(
    () =>
      fields
        .filter((field) => field.include_in_output && isNumericFieldType(field.field_type))
        .map((field) => field.key),
    [fields],
  );
  const fullDatasetOutlierTotal = useMemo(
    () =>
      visibleOutlierFieldKeys.reduce(
        (sum, key) => sum + (profile.find((field) => field.key === key)?.outlier_count_iqr ?? 0),
        0,
      ),
    [profile, visibleOutlierFieldKeys],
  );
  const histogramBinsForRefresh = useMemo(
    () =>
      Object.fromEntries(
        fields
          .filter((field) => isNumericFieldType(field.field_type))
          .map((field) => [field.key, Math.min(64, Math.max(2, histogramBinsByField[field.key] ?? 8))]),
      ),
    [fields, histogramBinsByField],
  );

  function applyProfileSnapshot(
    nextProfile: FieldProfile[],
    nextQuality: DatasetQualityReport | null,
    nextRecommendedWeights: Record<string, number>,
    nextWeightNotes: string[],
    nextMissingMatrixPreview: Array<{ id: string; missing_count: number; missing_fields: string[] }>,
    nextMissingRowsPreviewData: Array<{ id: string; values: Record<string, unknown> }>,
    nextCorrelationMatrix: Array<{ left_key: string; right_key: string; pearson: number; samples: number }>,
    nextDetailLevel: "summary" | "detailed",
  ) {
    setProfile(nextProfile);
    setQuality(nextQuality);
    setRecommendedWeights(nextRecommendedWeights);
    setWeightNotes(nextWeightNotes);
    setMissingMatrixPreview(nextMissingMatrixPreview);
    setMissingRowsPreviewData(nextMissingRowsPreviewData);
    setCorrelationMatrix(nextCorrelationMatrix);
    setProfileDetailLevel(nextDetailLevel);
  }

  async function loadRawStoredPreview(nextDatasetFileId: number, nextFilename: string) {
    try {
      const response = await fetchStoredProfile(nextDatasetFileId, nextFilename, {
        detailLevel: "summary",
      });
      setRawPreview(response.preview);
    } catch (previewError) {
      console.error(previewError);
    }
  }

  async function hydratePreprocessingStateFromStoredProfile(
    nextDatasetFileId: number,
    nextFilename: string,
    histogramSeed: Record<string, number> = {},
    resetTargetRow = false,
  ) {
    setProfileDetailsLoading(true);
    try {
      const response = await fetchStoredProfile(nextDatasetFileId, nextFilename, {
        histogramBinsByField: histogramSeed,
        detailLevel: "detailed",
      });
      const nextFields = buildInitialFields(response.preview, response.profile.fields);
      const nextHistogramBinsByField = Object.fromEntries(
        nextFields
          .filter((field) => isNumericFieldType(field.field_type))
          .map((field) => [field.key, Math.min(64, Math.max(2, histogramSeed[field.key] ?? 8))]),
      );

      setRawPreview(response.preview);
      setPreview(response.preview);
      applyProfileSnapshot(
        response.profile.fields,
        response.profile.quality,
        response.profile.recommended_weights,
        response.profile.weight_notes,
        response.profile.missing_matrix_preview ?? [],
        response.profile.missing_rows_preview ?? [],
        response.profile.correlation_matrix ?? [],
        response.profile.detail_level === "detailed" ? "detailed" : "summary",
      );
      setFields(nextFields);
      setHistogramBinsByField(nextHistogramBinsByField);
      setHistogramDraftBinsByField(nextHistogramBinsByField);
      setChartModeByField({});
      if (resetTargetRow || !targetRowId) {
        setTargetRowId(response.preview.normalized_dataset?.rows?.[0]?.id ?? "");
      }
    } catch (detailsError) {
      console.error(detailsError);
    } finally {
      setProfileDetailsLoading(false);
    }
  }

  function buildOrdinalMapForField(fieldKey: string) {
    const column = encodingPreview?.columns.find((item) => item.normalized_name === fieldKey);
    const samples = column ? uniqueSamples(column) : [];
    return buildOrdinalMap(samples) ?? { low: 0.25, medium: 0.5, high: 1 };
  }

  function updateFieldEncoding(index: number, encoding: FieldConfig["encoding"]) {
    const field = fields[index];
    if (!field) return;
    if (field.field_type === "categorical" && encoding !== "ordinal") {
      updateField(index, {
        encoding: "ordinal",
        ordinal_map: field.ordinal_map && Object.keys(field.ordinal_map).length ? field.ordinal_map : buildOrdinalMapForField(field.key),
      });
      return;
    }
    if (encoding === "ordinal") {
      updateField(index, {
        encoding,
        ordinal_map: field.ordinal_map && Object.keys(field.ordinal_map).length ? field.ordinal_map : buildOrdinalMapForField(field.key),
      });
      return;
    }
    if (encoding === "binary_map") {
      updateField(index, {
        encoding,
        binary_map: field.binary_map ?? { true: 1, false: 0, "1": 1, "0": 0 },
      });
      return;
    }
    updateField(index, { encoding });
  }

  function updateFieldNormalization(index: number, normalization: FieldConfig["normalization"]) {
    updateField(index, { normalization: normalization === "none" ? "minmax" : normalization });
  }

  function updateAnalysisMode(nextMode: AnalysisMode) {
    setAnalysisMode(nextMode);
    setCriteria((current) => applyModeDefaultsToCriteria(current, nextMode));
  }

  function applyHistogramBins(fieldKey: string) {
    setHistogramBinsByField((current) => {
      const nextValue = Math.min(64, Math.max(2, histogramDraftBinsByField[fieldKey] ?? current[fieldKey] ?? 8));
      return { ...current, [fieldKey]: nextValue };
    });
  }

  const visibleStages = user?.role === "admin" ? stages : stages.filter((stage) => stage.id !== "admin");

  function persistAuth(nextToken: string, nextUser: AuthUser) {
    setToken(nextToken);
    setUser(nextUser);
    localStorage.setItem("access_token", nextToken);
    localStorage.setItem("auth_user", JSON.stringify(nextUser));
    setAuthError(null);
    setAuthModalOpen(false);
  }

  function logout() {
    setToken("");
    setUser(null);
    setHistory([]);
    setAdminStats(null);
    setAdminUsers([]);
    setUserMenuOpen(false);
    localStorage.removeItem("access_token");
    localStorage.removeItem("auth_user");
  }

  function resetWorkflow() {
    setActiveStage("data");
    setFile(null);
    setDatasetFileId(null);
    setSourceFilename("");
    setRawPreview(null);
    setPreview(null);
    setProfile([]);
    setQuality(null);
    setRecommendedWeights({});
    setWeightNotes([]);
    setMissingMatrixPreview([]);
    setMissingRowsPreviewData([]);
    setCorrelationMatrix([]);
    setFields([]);
    setCriteria([]);
    setAnalysisMode("analog_search");
    setTargetRowId("");
    setGeoRadiusKm(null);
    setResult(null);
    setSelectedResultId("");
    setRawResultValuesById({});
    setResultsPage(1);
    setLastHistoryId(null);
    setActiveProjectId(null);
    setHistoryProjectFilter("all");
    setHistoryCompareIds([]);
    setScenarioTitle("Сценарий сравнительного анализа");
    setPreprocessingSection("types");
    setHistogramBinsByField({});
    setHistogramDraftBinsByField({});
    setChartModeByField({});
    setProfileDetailLevel("summary");
    setProfileDetailsLoading(false);
    setReportPreviewOpen(false);
    setError(null);
    localStorage.removeItem(WORKFLOW_STORAGE_KEY);
  }

  async function resetPreprocessing() {
    if (!datasetFileId || !sourceFilename) return;

    setLoading(true);
    setError(null);
    setRawPreview(null);
    setPreview(null);
    setProfile([]);
    setQuality(null);
    setRecommendedWeights({});
    setWeightNotes([]);
    setMissingMatrixPreview([]);
    setMissingRowsPreviewData([]);
    setCorrelationMatrix([]);
    setCriteria([]);
    setResult(null);
    setSelectedResultId("");
    setRawResultValuesById({});
    setResultsPage(1);
    setLastHistoryId(null);
    setPreprocessingSection("types");
    setFields([]);
    setTargetRowId("");
    setGeoRadiusKm(null);
    setHistogramBinsByField({});
    setHistogramDraftBinsByField({});
    setChartModeByField({});
    try {
      await hydratePreprocessingStateFromStoredProfile(datasetFileId, sourceFilename, {}, true);
    } finally {
      setLoading(false);
    }
  }

  function openUserSection(stage: StageId) {
    setUserMenuOpen(false);
    if (stage === "history") {
      void refreshHistory(true);
      return;
    }
    if (stage === "projects") {
      void refreshHistory(false);
    }
    setActiveStage(stage);
  }

  function validateAuthForm() {
    const email = authEmail.trim();
    const password = authPassword.trim();
    const name = authName.trim();
    if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) {
      return "Введите корректный email.";
    }
    if (password.length < 6) {
      return "Пароль должен содержать минимум 6 символов.";
    }
    if (authMode === "register" && name.length < 2) {
      return "Имя должно содержать минимум 2 символа.";
    }
    return null;
  }

  async function handleAuthSubmit() {
    const validationError = validateAuthForm();
    if (validationError) {
      setAuthError(validationError);
      return;
    }
    setLoading(true);
    setError(null);
    setAuthError(null);
    try {
      const response = authMode === "login"
        ? await login(authEmail.trim(), authPassword)
        : await register(authName.trim(), authEmail.trim(), authPassword);
      persistAuth(response.access_token, response.user);
      setActiveStage("data");
    } catch (nextAuthError) {
      setAuthError(nextAuthError instanceof Error ? nextAuthError.message : "Не удалось выполнить вход или регистрацию.");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    void restoreSession();
  }, []);

  useEffect(() => {
    if (!user) return;
    void refreshHistory(false);
  }, [user?.id]);

  useEffect(() => {
    if (result?.ranking.length && !result.ranking.some((item) => item.object_id === selectedResultId)) {
      setSelectedResultId(result.ranking[0].object_id);
    }
  }, [result, selectedResultId]);

  useEffect(() => {
    if (!datasetFileId || !result?.ranking.length) return;

    const pageStart = Math.max(0, (resultsPage - 1) * 8);
    const pageObjectIds = result.ranking
      .slice(pageStart, pageStart + 8)
      .map((item) => item.object_id)
      .filter((objectId) => !rawResultValuesById[objectId]);

    if (!pageObjectIds.length) return;

    let cancelled = false;

    void fetchRawObjects(datasetFileId, pageObjectIds, sourceFilename || preview?.filename || undefined)
      .then((response: RawObjectsResponse) => {
        if (cancelled || !response?.objects) return;
        setRawResultValuesById((current) => ({
          ...current,
          ...response.objects,
        }));
      })
      .catch((rawError) => {
        if (!cancelled) {
          console.error(rawError);
        }
      });

    return () => {
      cancelled = true;
    };
  }, [datasetFileId, result, resultsPage, sourceFilename, preview, rawResultValuesById]);

  useEffect(() => {
    if (!datasetFileId || !selectedResult?.object_id) return;
    if (rawResultValuesById[selectedResult.object_id]) return;

    let cancelled = false;

    void fetchRawObjects(datasetFileId, [selectedResult.object_id], sourceFilename || preview?.filename || undefined)
      .then((response: RawObjectsResponse) => {
        const objectValues = response.objects[selectedResult.object_id];
        if (cancelled || !objectValues) return;
        setRawResultValuesById((current) => ({
          ...current,
          [selectedResult.object_id]: objectValues,
        }));
      })
      .catch((rawError) => {
        if (!cancelled) {
          console.error(rawError);
        }
      });

    return () => {
      cancelled = true;
    };
  }, [datasetFileId, selectedResult, sourceFilename, preview, rawResultValuesById]);

  useEffect(() => {
    const totalPages = Math.max(1, Math.ceil((result?.ranking.length ?? 0) / 8));
    if (resultsPage > totalPages) {
      setResultsPage(totalPages);
    }
  }, [result, resultsPage]);

  useEffect(() => {
    if (activeStage !== "preprocessing") return;
    if (!preview) return;
    if (!fields.length) {
      if (datasetFileId && sourceFilename) {
        void hydratePreprocessingStateFromStoredProfile(datasetFileId, sourceFilename, histogramBinsByField);
      }
      return;
    }
    if (!datasetFileId || !sourceFilename) return;
    if (profileDetailLevel === "detailed" || profileDetailsLoading) return;
    void hydratePreprocessingStateFromStoredProfile(datasetFileId, sourceFilename, histogramBinsByField);
  }, [activeStage, datasetFileId, sourceFilename, profileDetailLevel, profileDetailsLoading, preview, fields, profile, histogramBinsByField, targetRowId]);

  useEffect(() => {
    if (activeStage !== "criteria") return;
    if (!fields.length) return;
    const nextCriteria = syncCriteriaWithFields(fields, criteria, recommendedWeights, analysisMode);
    const hasChanged =
      nextCriteria.length !== criteria.length
      || nextCriteria.some((item, index) => {
        const current = criteria[index];
        return (
          !current
          || current.key !== item.key
          || current.name !== item.name
          || current.weight !== item.weight
          || current.direction !== item.direction
          || JSON.stringify(current.scale_map ?? null) !== JSON.stringify(item.scale_map ?? null)
          || current.target_value !== item.target_value
        );
      });
    if (hasChanged) {
      setCriteria(nextCriteria);
    }
  }, [activeStage, fields, criteria, recommendedWeights, analysisMode]);

  useEffect(() => {
    if (activeStage !== "preprocessing") return;
    if (!fields.length) return;
    setFields((current) => {
      let changed = false;
      const next = current.map((field) => {
        if (field.field_type !== "categorical") return field;
        const column = encodingPreview?.columns.find((item) => item.normalized_name === field.key);
        const categories = rawCategoryCountsForField(encodingPreview, field.key, column).map((item) => item.label);
        const nextOrdinalMap = mergeOrdinalMap(categories, field.ordinal_map);
        const sameMap =
          Object.keys(nextOrdinalMap).length === Object.keys(field.ordinal_map ?? {}).length
          && Object.entries(nextOrdinalMap).every(([key, value]) => field.ordinal_map?.[key] === value);
        if (field.encoding !== "ordinal" || !sameMap) {
          changed = true;
          return {
            ...field,
            encoding: "ordinal",
            ordinal_map: nextOrdinalMap,
          };
        }
        return field;
      });
      return changed ? next : current;
    });
  }, [activeStage, fields.length, encodingPreview]);

  useEffect(() => {
    const state: SavedWorkflowState = {
      activeStage,
      datasetFileId,
      rawPreview,
      preview,
      profile,
      quality,
      recommendedWeights,
      weightNotes,
      missingMatrixPreview,
      missingRowsPreviewData,
      correlationMatrix,
      fields,
      criteria,
      analysisMode,
      targetRowId,
      geoRadiusKm,
      result: compactPipelineResultForStorage(result),
      activeProjectId,
      historyProjectFilter,
      scenarioTitle,
      sourceFilename,
      preprocessingSection,
      histogramBinsByField,
      lastHistoryId,
      profileDetailLevel,
    };
    try {
      localStorage.setItem(WORKFLOW_STORAGE_KEY, JSON.stringify(state));
    } catch (storageError) {
      console.warn("Не удалось сохранить полное состояние workflow, сохраняем облегченный снимок.", storageError);
      const fallbackState: SavedWorkflowState = {
        activeStage,
        datasetFileId,
        rawPreview,
        preview,
        profile,
        quality,
        recommendedWeights,
        weightNotes,
        missingMatrixPreview,
        missingRowsPreviewData,
        correlationMatrix,
        fields,
        criteria,
        analysisMode,
        targetRowId,
        geoRadiusKm,
        result: null,
        activeProjectId,
        historyProjectFilter,
        scenarioTitle,
        sourceFilename,
        preprocessingSection,
        histogramBinsByField,
        lastHistoryId,
        profileDetailLevel,
      };
      try {
        localStorage.setItem(WORKFLOW_STORAGE_KEY, JSON.stringify(fallbackState));
      } catch {
        localStorage.removeItem(WORKFLOW_STORAGE_KEY);
      }
    }
  }, [
    activeStage,
    datasetFileId,
    rawPreview,
    preview,
    profile,
    quality,
    recommendedWeights,
    weightNotes,
    missingMatrixPreview,
    missingRowsPreviewData,
    correlationMatrix,
    fields,
    criteria,
    analysisMode,
    targetRowId,
    result,
    activeProjectId,
    historyProjectFilter,
    scenarioTitle,
    sourceFilename,
    preprocessingSection,
    histogramBinsByField,
    lastHistoryId,
    profileDetailLevel,
  ]);

  async function refreshHistory(openStage = true) {
    if (!user) return;
    setLoading(true);
    setError(null);
    try {
      const [nextHistory, nextProjects] = await Promise.all([fetchHistory(user.id), fetchProjects(user.id)]);
      setHistory(nextHistory);
      setProjects(nextProjects);
      if (!activeProjectId && nextProjects.length > 0) {
        setActiveProjectId(nextProjects[0].id);
      }
      if (openStage) {
        setActiveStage("history");
      }
    } catch (historyError) {
      setError(historyError instanceof Error ? historyError.message : "Не удалось загрузить историю");
    } finally {
      setLoading(false);
    }
  }

  async function refreshAdminDashboard() {
    if (!token || user?.role !== "admin") return;
    setLoading(true);
    setError(null);
    try {
      const [stats, users, dashboard] = await Promise.all([
        fetchAdminStats(token),
        fetchAdminUsers(token),
        fetchSystemDashboard(token),
      ]);
      setAdminStats(stats);
      setAdminUsers(users);
      setSystemDashboard(dashboard);
      setActiveStage("admin");
    } catch (adminError) {
      setError(adminError instanceof Error ? adminError.message : "Не удалось загрузить админ-панель");
    } finally {
      setLoading(false);
    }
  }

  async function handleCreateProject() {
    if (!user || !newProjectName.trim()) return;
    setLoading(true);
    setError(null);
    try {
      const project = await createProject(user, newProjectName.trim(), "Пользовательский проект сравнительного анализа");
      setProjects((current) => [project, ...current]);
      await handleProjectSelection(project.id);
    } catch (projectError) {
      setError(projectError instanceof Error ? projectError.message : "Не удалось создать проект");
    } finally {
      setLoading(false);
    }
  }

  async function handleProjectSelection(projectId: number | null) {
    setActiveProjectId(projectId);
    setError(null);
    setResult(null);
    setSelectedResultId("");
    setRawResultValuesById({});
    if (!projectId || !user) {
      setFile(null);
      setDatasetFileId(null);
      setSourceFilename("");
      setRawPreview(null);
      setPreview(null);
      setProfile([]);
      setQuality(null);
      setRecommendedWeights({});
      setWeightNotes([]);
      setMissingMatrixPreview([]);
      setMissingRowsPreviewData([]);
      setCorrelationMatrix([]);
      setFields([]);
      setCriteria([]);
      setTargetRowId("");
      setLastHistoryId(null);
      return;
    }

    setLoading(true);
    try {
      const historySource = history.length ? history : await fetchHistory(user.id);
      if (!history.length) {
        setHistory(historySource);
      }
      const latest = [...historySource]
        .filter((item) => item.project_id === projectId)
        .sort((left, right) => Date.parse(right.created_at) - Date.parse(left.created_at))[0];

      if (!latest) {
        setFile(null);
        setDatasetFileId(null);
        setSourceFilename("");
        setRawPreview(null);
        setPreview(null);
        setProfile([]);
        setQuality(null);
        setRecommendedWeights({});
        setWeightNotes([]);
        setMissingMatrixPreview([]);
        setMissingRowsPreviewData([]);
        setCorrelationMatrix([]);
        setFields([]);
        setCriteria([]);
        setTargetRowId("");
        setLastHistoryId(null);
        setScenarioTitle("Сценарий сравнительного анализа");
        setHistogramBinsByField({});
        setHistogramDraftBinsByField({});
        setChartModeByField({});
        setHistogramBinsByField({});
        setHistogramDraftBinsByField({});
        setChartModeByField({});
        return;
      }

      const parameters = parseHistoryParameters(latest);
      if (!parameters) {
        throw new Error("Не удалось прочитать настройки выбранного проекта.");
      }

      const nextFields = Array.isArray(parameters.fields) ? parameters.fields : [];
      const nextCriteria = Array.isArray(parameters.criteria) ? parameters.criteria : [];
      const normalizedFields = enforceRequiredScaling(nextFields);

      setFile(null);
      setFields(normalizedFields);
      setCriteria(nextCriteria.length ? nextCriteria : criteriaDefaults(normalizedFields, recommendedWeights, parameters.analysis_mode === "rating" ? "rating" : "analog_search"));
      setTargetRowId(parameters.target_row_id ?? "");
      setGeoRadiusKm(parameters.geo_radius_km ?? null);
      setAnalysisMode(parameters.analysis_mode === "rating" ? "rating" : "analog_search");
      setScenarioTitle(parameters.scenario_title ?? latest.title);
      setDatasetFileId(latest.dataset_file_id ?? null);
      setSourceFilename(latest.source_filename ?? "");
      setLastHistoryId(latest.id);

      if (latest.dataset_file_id && nextFields.length) {
        const nextHistogramBinsByField = Object.fromEntries(
          normalizedFields
            .filter((field) => isNumericFieldType(field.field_type))
            .map((field) => [field.key, Math.min(64, Math.max(2, histogramBinsByField[field.key] ?? 8))]),
        );
        const response = await refreshPreprocessing(latest.dataset_file_id, normalizedFields, latest.source_filename ?? undefined, {
          histogramBinsByField: nextHistogramBinsByField,
        });
        setRawPreview((current) => current ?? response.preview);
        setPreview(response.preview);
        setProfile(response.profile.fields);
        setQuality(response.profile.quality);
        setRecommendedWeights(response.profile.recommended_weights);
        setWeightNotes(response.profile.weight_notes);
        setMissingMatrixPreview(response.profile.missing_matrix_preview ?? []);
        setMissingRowsPreviewData(response.profile.missing_rows_preview ?? []);
        setCorrelationMatrix(response.profile.correlation_matrix ?? []);
        setHistogramBinsByField(nextHistogramBinsByField);
        setHistogramDraftBinsByField(nextHistogramBinsByField);
        setChartModeByField({});
      } else {
        setRawPreview(null);
        setPreview(null);
        setProfile([]);
        setQuality(null);
        setRecommendedWeights({});
        setWeightNotes([]);
        setMissingMatrixPreview([]);
        setMissingRowsPreviewData([]);
        setCorrelationMatrix([]);
        setHistogramBinsByField({});
        setHistogramDraftBinsByField({});
        setChartModeByField({});
      }
    } catch (selectionError) {
      setError(selectionError instanceof Error ? selectionError.message : "Не удалось загрузить настройки проекта");
    } finally {
      setLoading(false);
    }
  }

  function toggleHistoryCompare(id: number) {
    setHistoryCompareIds((current) => {
      if (current.includes(id)) {
        return current.filter((item) => item !== id);
      }
      return [...current.slice(-1), id];
    });
  }

  function applyHistoryScenario(item: ComparisonHistoryItem) {
    const parameters = parseHistoryParameters(item);
    if (!parameters) {
      setError("Не удалось применить сценарий: сохраненные параметры повреждены.");
      return;
    }
    if (Array.isArray(parameters.fields)) {
      setFields(enforceRequiredScaling(parameters.fields));
    }
    if (Array.isArray(parameters.criteria)) {
      setCriteria(parameters.criteria);
    }
    setTargetRowId(parameters.target_row_id ?? "");
    setGeoRadiusKm(parameters.geo_radius_km ?? null);
    updateAnalysisMode(parameters.analysis_mode === "rating" ? "rating" : "analog_search");
    setActiveProjectId(parameters.project_id ?? item.project_id ?? null);
    setScenarioTitle(`${item.title} (повтор)`);
    setActiveStage("criteria");
    setError(file && preview ? null : "Параметры сценария применены. Для повторного расчета загрузите исходный файл на этапе «Данные».");
  }

  async function restoreSession() {
    if (!token) return;
    try {
      const currentUser = await fetchMe(token);
      persistAuth(token, currentUser);
    } catch {
      logout();
    }
  }

  async function handlePreview() {
    if (!file) return;
    setLoading(true);
    setError(null);
    setResult(null);
    setSelectedResultId("");
    setRawResultValuesById({});
    try {
      const response = await profileFile(file);
      setDatasetFileId(response.dataset_file_id ?? null);
      setSourceFilename(file.name);
      setRawPreview(response.preview);
      setPreview(response.preview);
      setProfile([]);
      setQuality(null);
      setRecommendedWeights({});
      setWeightNotes([]);
      setMissingMatrixPreview([]);
      setMissingRowsPreviewData([]);
      setCorrelationMatrix([]);
      setProfileDetailLevel("summary");
      setFields([]);
      setCriteria([]);
      setHistogramBinsByField({});
      setHistogramDraftBinsByField({});
      setChartModeByField({});
      const firstRowId = response.preview.normalized_dataset?.rows?.[0]?.id ?? "";
      setTargetRowId(firstRowId);
    } catch (previewError) {
      setError(previewError instanceof Error ? previewError.message : "Не удалось построить предпросмотр файла");
    } finally {
      setLoading(false);
    }
  }

  async function handleRun() {
    if (!file && !datasetFileId) return;
    setLoading(true);
    setError(null);
    try {
      const effectiveFields = enforceRequiredScaling(fields);
      if (effectiveFields !== fields) {
        setFields(effectiveFields);
      }
      const response = await runPipeline(
        file,
        effectiveFields,
        criteria,
        analysisMode === "analog_search" ? targetRowId : undefined,
        analysisMode === "analog_search" ? geoRadiusKm : null,
        analysisMode,
        token || undefined,
        activeProjectId,
        scenarioTitle,
        undefined,
        datasetFileId,
        sourceFilename || file?.name,
        undefined,
        false,
        10,
        Math.max(preview?.rows_total ?? 0, 10),
      );
      // Validate response shape to avoid runtime exceptions in the UI
      if (!response || !Array.isArray(response.ranking)) {
        throw new Error("Неверный формат ответа от сервера: отсутствует поле ranking");
      }
      setResult(normalizePipelineResult(response));
      setRawResultValuesById({});
      // Guard access to first item
      setSelectedResultId(response.ranking.length > 0 && response.ranking[0] && response.ranking[0].object_id ? response.ranking[0].object_id : "");
      setResultsPage(1);
      setLastHistoryId(response.history_id ?? null);
      setActiveStage("results");
      if (user) {
        void refreshHistory(false);
      }
    } catch (runError) {
      setError(runError instanceof Error ? runError.message : "Не удалось выполнить расчет");
    } finally {
      setLoading(false);
    }
  }

  function updateField(index: number, patch: Partial<FieldConfig>) {
    setFields((current) => current.map((item, idx) => (idx === index ? { ...item, ...patch } : item)));
  }

  async function applyPreprocessingSection(section: PrepSectionId) {
    if (!datasetFileId) {
      setError("Сначала загрузите датасет.");
      return;
    }
    setApplyingSection(section);
    setLoading(true);
    setError(null);
    try {
      const effectiveFields = enforceRequiredScaling(fields);
      if (effectiveFields !== fields) {
        setFields(effectiveFields);
      }
      const response = await refreshPreprocessing(datasetFileId, effectiveFields, sourceFilename || preview?.filename || undefined, {
        histogramBinsByField: histogramBinsForRefresh,
        detailLevel: "summary",
      });
      setPreview(response.preview);
      applyProfileSnapshot(
        response.profile.fields,
        response.profile.quality,
        response.profile.recommended_weights,
        response.profile.weight_notes,
        response.profile.missing_matrix_preview ?? [],
        response.profile.missing_rows_preview ?? [],
        response.profile.correlation_matrix ?? [],
        response.profile.detail_level === "detailed" ? "detailed" : "summary",
      );
      setProfileDetailsLoading(true);
      void refreshPreprocessing(datasetFileId, effectiveFields, sourceFilename || preview?.filename || undefined, {
        histogramBinsByField: histogramBinsForRefresh,
        detailLevel: "detailed",
      })
        .then((detailedResponse) => {
          setPreview(detailedResponse.preview);
          applyProfileSnapshot(
            detailedResponse.profile.fields,
            detailedResponse.profile.quality,
            detailedResponse.profile.recommended_weights,
            detailedResponse.profile.weight_notes,
            detailedResponse.profile.missing_matrix_preview ?? [],
            detailedResponse.profile.missing_rows_preview ?? [],
            detailedResponse.profile.correlation_matrix ?? [],
            detailedResponse.profile.detail_level === "detailed" ? "detailed" : "summary",
          );
        })
        .catch((detailsError) => {
          console.error(detailsError);
        })
        .finally(() => {
          setProfileDetailsLoading(false);
        });
    } catch (applyError) {
      setError(applyError instanceof Error ? applyError.message : "Не удалось применить настройки предобработки");
    } finally {
      setLoading(false);
      setApplyingSection(null);
    }
  }

  function applyRecommendedFieldSettings(index: number) {
    const field = fields[index];
    if (!field) return;
    const fieldProfile = profile.find((item) => item.key === field.key);
    if (fieldProfile) {
      updateField(index, { ...fieldProfile.recommended_config, use_in_label: field.use_in_label });
      return;
    }
    if (isNumericFieldType(field.field_type)) {
      updateField(index, {
        missing_strategy: "median",
        outlier_method: "iqr_clip",
        normalization: "minmax",
        encoding: "none",
        rounding_precision: field.field_type === "float" ? (field.rounding_precision ?? 2) : null,
      });
      return;
    }
    if (field.field_type === "categorical") {
      updateField(index, {
        missing_strategy: "mode",
        outlier_method: "none",
        normalization: "none",
        encoding: "ordinal",
      });
      return;
    }
    if (field.field_type === "binary") {
      updateField(index, {
        missing_strategy: "mode",
        outlier_method: "none",
        normalization: "none",
        encoding: "none",
      });
    }
  }

  function updateCriterion(index: number, patch: Partial<CriterionConfig>) {
    setCriteria((current) => current.map((item, idx) => (idx === index ? { ...item, ...patch } : item)));
  }

  function normalizeWeights() {
    setCriteria((current) => {
      const total = current.reduce((sum, item) => sum + Number(item.weight || 0), 0);
      if (total <= 0) return current;
      const normalized = current.map((item) => ({ ...item, weight: Number((item.weight / total).toFixed(4)) }));
      const drift = Number((1 - normalized.reduce((sum, item) => sum + item.weight, 0)).toFixed(4));
      if (normalized.length && drift) {
        normalized[0] = { ...normalized[0], weight: Number((normalized[0].weight + drift).toFixed(4)) };
      }
      return normalized;
    });
  }

  function setEqualWeights() {
    setCriteria((current) => {
      if (!current.length) return current;
      const weight = Number((1 / current.length).toFixed(4));
      const patched = current.map((item) => ({ ...item, weight }));
      return patched;
    });
  }

  function applyRecommendedWeights() {
    setCriteria((current) => {
      const recommended = criteriaDefaults(fields, recommendedWeights, analysisMode);
      const currentByKey = new Map(current.map((item) => [item.key, item]));
      const merged = recommended.map((item) => {
        const existing = currentByKey.get(item.key);
        return {
          ...item,
          name: existing?.name || item.name,
          direction: analysisMode === "analog_search" ? "target" : (existing?.direction ?? item.direction),
          scale_map: existing?.scale_map ?? item.scale_map,
          target_value: existing?.target_value ?? item.target_value,
        };
      });
      return normalizeCriteriaWeights(merged);
    });
  }

  function changeResultsPage(page: number) {
    if (!result) return;
    const totalPages = Math.max(1, Math.ceil(result.ranking.length / 8));
    const nextPage = Math.min(Math.max(1, page), totalPages);
    setResultsPage(nextPage);
    const pageStart = (nextPage - 1) * 8;
    const pageItem = result.ranking[pageStart];
    if (pageItem) {
      setSelectedResultId(pageItem.object_id);
    }
  }

  function buildCurrentReportHtml() {
    return result ? buildHtmlReport(result, criteria) : "";
  }

  function openReportPreview() {
    if (!result) return;
    setReportPreviewHtml(buildCurrentReportHtml());
    setReportPreviewOpen(true);
  }

  function rebuildCriteria() {
    setCriteria(criteriaDefaults(fields, recommendedWeights, analysisMode));
    setActiveStage("criteria");
  }

  function exportHtmlReport() {
    if (!result) return;
    downloadBlob("comparison-report.html", buildCurrentReportHtml(), "text/html;charset=utf-8");
  }

  async function storeReportFile(blob: Blob, filename: string) {
    if (!lastHistoryId) return;
    const reportFileId = await uploadReportFile(filename, blob);
    await bindReportToHistory(lastHistoryId, reportFileId);
    if (user) {
      void refreshHistory(false);
    }
  }

  async function exportDocxReport() {
    if (!result) return;
    setLoading(true);
    setError(null);
    try {
      await downloadDocxReport(result, criteria);
    } catch (reportError) {
      setError(reportError instanceof Error ? reportError.message : "Не удалось скачать DOCX-отчет");
    } finally {
      setLoading(false);
    }
  }

  async function exportPdfReport() {
    if (!result) return;
    setLoading(true);
    setError(null);
    const mountNode = document.createElement("div");
    try {
      const reportHtml = buildCurrentReportHtml();
      const parser = new DOMParser();
      const parsed = parser.parseFromString(reportHtml, "text/html");
      const bodyMarkup = parsed.body?.innerHTML || reportHtml;
      mountNode.className = "report-pdf-mount";
      mountNode.innerHTML = bodyMarkup;
      Object.assign(mountNode.style, {
        position: "fixed",
        left: "-10000px",
        top: "0",
        width: "860px",
        background: "#ffffff",
        padding: "24px",
        zIndex: "-1",
      });
      document.body.appendChild(mountNode);

      const [{ jsPDF }, { default: html2canvas }] = await Promise.all([import("jspdf"), import("html2canvas")]);
      const documentPdf = new jsPDF({ orientation: "portrait", unit: "pt", format: "a4" });

      const canvas = await html2canvas(mountNode, {
        scale: 2,
        useCORS: true,
        backgroundColor: "#ffffff",
      });

      const marginLeft = 20;
      const marginTop = 24;
      const marginBottom = 24;
      const pageWidth = documentPdf.internal.pageSize.getWidth();
      const pageHeight = documentPdf.internal.pageSize.getHeight();
      const contentWidth = pageWidth - marginLeft * 2;
      const contentHeight = pageHeight - marginTop - marginBottom;

      const pxPerPt = canvas.width / contentWidth;
      const pageHeightPx = Math.max(1, Math.floor(contentHeight * pxPerPt));
      let renderedPx = 0;
      let pageIndex = 0;

      while (renderedPx < canvas.height) {
        const sliceHeightPx = Math.min(pageHeightPx, canvas.height - renderedPx);
        const pageCanvas = document.createElement("canvas");
        pageCanvas.width = canvas.width;
        pageCanvas.height = sliceHeightPx;

        const context = pageCanvas.getContext("2d");
        if (!context) {
          throw new Error("Не удалось сформировать PDF-страницу");
        }

        context.drawImage(canvas, 0, renderedPx, canvas.width, sliceHeightPx, 0, 0, canvas.width, sliceHeightPx);
        const imageData = pageCanvas.toDataURL("image/png");
        const sliceHeightPt = (sliceHeightPx / canvas.width) * contentWidth;

        if (pageIndex > 0) {
          documentPdf.addPage();
        }

        documentPdf.addImage(imageData, "PNG", marginLeft, marginTop, contentWidth, sliceHeightPt, undefined, "FAST");
        renderedPx += sliceHeightPx;
        pageIndex += 1;
      }

      const blob = documentPdf.output("blob");
      const fileName = "comparison-report.pdf";
      const url = URL.createObjectURL(blob);
      const link = document.createElement("a");
      link.href = url;
      link.download = fileName;
      link.click();
      URL.revokeObjectURL(url);

      await storeReportFile(blob, fileName);
    } catch (reportError) {
      setError(reportError instanceof Error ? reportError.message : "Не удалось сформировать PDF-отчет");
    } finally {
      mountNode.remove();
      setLoading(false);
    }
  }

  function exportJsonReport() {
    if (!result) return;
    downloadBlob(
      "comparison-report.json",
      JSON.stringify({ generated_at: new Date().toISOString(), fields, criteria, result }, null, 2),
      "application/json;charset=utf-8",
    );
  }

  function exportSortedDatasetCsv() {
    if (!result) return;
    const csv = buildSortedDatasetCsv(result, criteria, analysisMode);
    downloadBlob("sorted-scored-dataset.csv", csv, "text/csv;charset=utf-8");
  }

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div className="brand">
          <div className="brand-mark">CA</div>
          <div>
            <strong>CompareLab</strong>
            <span>Аналитическая Система</span>
          </div>
        </div>

        <nav className="stage-nav">
          {visibleStages.map((stage, index) => (
            <button
              className={`stage-link ${activeStage === stage.id ? "active" : ""}`}
              key={stage.id}
              onClick={() => setActiveStage(stage.id)}
            >
              <span className={`stage-index ${completedStages[stage.id] ? "done" : ""}`}>{index + 1}</span>
              <span>
                <strong>{stage.title}</strong>
                <small>{stage.caption}</small>
              </span>
            </button>
          ))}
        </nav>

      </aside>

      <main className="workspace">
        <header className="topbar">
          <div>
            <h1>Сравнительный анализ объектов</h1>
            {profileDetailsLoading ? <p className="muted-note">Обновляем данные...</p> : null}
          </div>
          <div className="topbar-actions">
            <button className="ghost-button" onClick={resetWorkflow}>Новый расчет</button>
            <button onClick={handleRun} disabled={(!file && !datasetFileId) || !preview || criteria.length === 0 || loading}>
              {loading ? "Выполняется..." : "Запустить расчет"}
            </button>
            {user ? (
              <div className="user-menu">
                <button className="user-menu-trigger" onClick={() => setUserMenuOpen((value) => !value)}>
                  <span className="avatar">{user.full_name.slice(0, 1).toUpperCase()}</span>
                  <span className="user-meta">
                    <strong>{user.full_name}</strong>
                    <span>{user.role === "admin" ? "Администратор" : "Пользователь"}</span>
                  </span>
                  <span className="menu-caret">▾</span>
                </button>
                {userMenuOpen ? (
                  <div className="user-dropdown">
                    <button onClick={() => openUserSection("projects")}>Проекты</button>
                    <button onClick={() => openUserSection("history")}>История сравнений</button>
                    {user.role === "admin" ? <button onClick={() => openUserSection("admin")}>Админ-панель</button> : null}
                    <button onClick={logout}>Выйти</button>
                  </div>
                ) : null}
              </div>
            ) : (
              <button
                className="login-button"
                onClick={() => {
                  setAuthMode("login");
                  setAuthError(null);
                  setAuthModalOpen(true);
                }}
              >
                Войти
              </button>
            )}
          </div>
        </header>

        {error ? <div className="alert error">{error}</div> : null}

        {activeStage === "data" ? (
          <section className="data-stage">
            {user ? (
              <div className="panel project-entry-panel">
                <div className="panel-head">
                  <div>
                    <span className="section-kicker">Проект</span>
                    <h2>Выберите проект перед началом</h2>
                  </div>
                </div>
                <div className="project-panel">
                  <label>
                    Текущий проект
                    <select
                      value={activeProjectId ?? ""}
                      onChange={(event) => void handleProjectSelection(event.target.value ? Number(event.target.value) : null)}
                    >
                      <option value="">Без проекта</option>
                      {projects.map((project) => (
                        <option key={project.id} value={project.id}>{project.name}</option>
                      ))}
                    </select>
                  </label>
                  <label>
                    Новый проект
                    <input value={newProjectName} onChange={(event) => setNewProjectName(event.target.value)} placeholder="Например: ЖК Май 2026" />
                  </label>
                  <button className="ghost-button" onClick={handleCreateProject} disabled={loading || !newProjectName.trim()}>
                    Создать проект
                  </button>
                </div>
                {activeProjectId && activeProjectLatestHistory ? (
                  <p className="muted-note">Подтянут сценарий: {activeProjectLatestHistory.title} от {new Date(activeProjectLatestHistory.created_at).toLocaleString("ru-RU")}</p>
                ) : null}
              </div>
            ) : null}

            <div className="panel hero-panel">
              <div className="hero-copy">
                <span className="section-kicker">Этап 1</span>
                <h2>Загрузка исходного датасета</h2>
                <p>Загрузите датасет в формате CSV, XLSX или JSON.</p>
              </div>
              <div className="upload-card">
                <label className="dropzone">
                  <input
                    type="file"
                    accept=".csv,.xlsx,.json"
                    onChange={(event) => {
                      const selectedFile = event.target.files?.[0] ?? null;
                      setFile(selectedFile);
                      setDatasetFileId(null);
                      setRawPreview(null);
                      setPreview(null);
                      if (selectedFile) {
                        setSourceFilename(selectedFile.name);
                      }
                    }}
                  />
                  <span>Выберите файл или перетащите его сюда</span>
                  <strong>
                    {file
                      ? file.name
                      : sourceFilename
                        ? datasetFileId
                          ? `${sourceFilename} · файл сохранен в хранилище`
                          : `${sourceFilename} · выберите файл заново для повторного расчета`
                        : "Файл не выбран"}
                  </strong>
                </label>
                <button className="wide-button" onClick={handlePreview} disabled={!file || loading}>
                  {loading ? "Загружаем предпросмотр..." : "Построить предпросмотр"}
                </button>
              </div>
            </div>

            <div className="panel">
                <div className="panel-head">
                  <div>
                    <span className="section-kicker">Предпросмотр</span>
                    <h2>Первые строки датасета</h2>
                  </div>
                  <div className="panel-head-actions">
                    {rawPreview ? <span className="pill">{rawPreview.rows_total} строк</span> : null}
                    {rawPreview ? <span className="pill">Исходный датасет</span> : null}
                  </div>
                </div>
              {rawPreview ? (
                <>
                  {rawPreview.warnings.length > 0 ? (
                    <div className="alert warning">
                      {rawPreview.warnings.slice(0, 4).map((warning) => (
                        <p key={warning}>{warning}</p>
                      ))}
                    </div>
                  ) : null}
                  <div className="table-wrap dataset-table wide">
                    <table>
                      <thead>
                        <tr>
                          {rawPreview.columns.map((column) => (
                            <th key={column.normalized_name}>{column.normalized_name}</th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {rawPreview.preview_rows.map((row, rowIndex) => (
                          <tr key={`preview-row-${rowIndex}`}>
                            {rawPreview.columns.map((column) => (
                              <td key={`${rowIndex}-${column.normalized_name}`}>
                                {row[column.normalized_name] === null || row[column.normalized_name] === ""
                                  ? <span className="muted-cell">Пусто</span>
                                  : String(row[column.normalized_name])}
                              </td>
                            ))}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </>
              ) : (
                <div className="empty-state">Загрузите файл, чтобы увидеть структуру датасета.</div>
              )}
            </div>

            {rawPreview ? (
              <div className="panel schema-panel">
                <div className="panel-head">
                  <div>
                    <span className="section-kicker">Паспорт данных</span>
                    <h2>Колонки и типы признаков</h2>
                  </div>
                  <button onClick={() => setActiveStage("preprocessing")}>Перейти к подготовке</button>
                </div>
                <div className="table-wrap schema-table-wrap">
                  <table className="schema-table">
                    <thead>
                      <tr>
                        <th>Поле</th>
                        <th>Тип</th>
                        <th>Пропуски</th>
                        <th>Уник.</th>
                        <th>Примеры</th>
                      </tr>
                    </thead>
                    <tbody>
                      {rawPreview.columns.map((column) => (
                        <tr key={column.normalized_name}>
                          <td>{column.normalized_name}</td>
                          <td><span className="tag">{inferUiType(column)}</span></td>
                          <td>{column.missing_count}</td>
                          <td>{column.unique_count}</td>
                          <td>{column.sample_values.slice(0, 3).map(String).join(", ")}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            ) : null}
          </section>
        ) : null}

        {activeStage === "preprocessing" ? (
          <section className="panel">
            <div className="panel-head">
              <div>
                <span className="section-kicker">Этап 2</span>
                <h2>Подготовка данных</h2>
              </div>
              <div className="panel-head-actions">
                <button type="button" className="ghost-button" onClick={resetPreprocessing} disabled={loading || fields.length === 0}>
                  Сбросить подготовку
                </button>
                <button onClick={rebuildCriteria} disabled={fields.length === 0}>Сформировать критерии</button>
              </div>
            </div>

            {fields.length > 0 ? (
              <>
                {quality ? (
                  <div className={`quality-card ${quality.level}`}>
                    <div className="quality-score">
                      <span>
                        <LabelWithTip
                          label="Индекс качества датасета"
                          tip={
                            <>
                              <span>Интегральная оценка готовности данных к сравнению.</span>
                              <Formula latex={"Q = 100 - (P_{missing} + P_{outliers} + P_{structure})"} />
                              <span>где P-параметры представляют штрафы за пропуски, выбросы и структурные ограничения датасета.</span>
                            </>
                          }
                        />
                      </span>
                      <strong>{quality.score.toFixed(0)}</strong>
                      <small>{translateQualityText(quality.readiness_label)}</small>
                    </div>
                    <div className="quality-body">
                      <div className="quality-metrics">
                        <div><span><LabelWithTip label="Строк" tip="Общее количество строк в текущем наборе данных." /></span><strong>{preview?.rows_total ?? 0}</strong></div>
                        <div><span><LabelWithTip label="Аналитических полей" tip="Количество полей, признанных пригодными для количественного сравнительного анализа." /></span><strong>{quality.analytic_fields_count}</strong></div>
                        <div><span><LabelWithTip label="Пропусков" tip="Суммарное количество отсутствующих значений в текущем наборе данных." /></span><strong>{quality.total_missing_values}</strong></div>
                        <div><span><LabelWithTip label="Выбросов IQR" tip={<><span>Количество наблюдений за границами межквартильного правила:</span><CompactFormula>x &lt; Q<sub>1</sub> - 1.5·IQR</CompactFormula><CompactFormula>x &gt; Q<sub>3</sub> + 1.5·IQR</CompactFormula><CompactFormula>IQR = Q<sub>3</sub> - Q<sub>1</sub></CompactFormula></>} /></span><strong>{fullDatasetOutlierTotal}</strong></div>
                        <div><span><LabelWithTip label="Текстовых полей" tip="Количество полей свободного текста, которые обычно имеют ограниченную пригодность для прямой числовой агрегации." /></span><strong>{quality.text_fields_count}</strong></div>
                      </div>
                    </div>
                  </div>
                ) : null}
                <div className="prep-tabs">
                  {prepSections.map((section) => (
                    <button
                      key={section.id}
                      className={`prep-tab ${preprocessingSection === section.id ? "active" : ""}`}
                      onClick={() => setPreprocessingSection(section.id)}
                    >
                      <strong>{section.title}</strong>
                      <span>{section.caption}</span>
                    </button>
                  ))}
                </div>
                {preprocessingSection === "preview" ? (
                  <>
                    <div className="prep-section-head">
                      <p>Проверьте текущий вид датасета после выбранных настроек.</p>
                      <button onClick={() => applyPreprocessingSection("preview")} disabled={loading || !datasetFileId}>
                        {applyingSection === "preview" ? "Применяем..." : "Применить"}
                      </button>
                    </div>
                    {preview ? (
                      <>
                        <div className="table-wrap dataset-table wide">
                          <table>
                            <thead>
                              <tr>
                                <th>ID</th>
                                {visiblePreviewColumns.map((column) => (
                                  <th key={`prep-preview-head-${column.normalized_name}`}>{column.normalized_name}</th>
                                ))}
                              </tr>
                            </thead>
                            <tbody>
                              {transformedPreviewRows.map((row) => (
                                <tr key={`prep-preview-row-${row.id}`}>
                                  <td>{row.id}</td>
                                  {visiblePreviewColumns.map((column) => {
                                    const value = row.values[column.normalized_name];
                                    return (
                                      <td key={`${row.id}-${column.normalized_name}`}>
                                        {value === null || value === undefined || value === ""
                                          ? <span className="muted-cell">Пусто</span>
                                          : formatPreviewCell(value)}
                                      </td>
                                    );
                                  })}
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                      </>
                    ) : (
                      <p className="muted-note">Предпросмотр появится после загрузки и применения настроек.</p>
                    )}
                  </>
                ) : null}

                {preprocessingSection === "types" ? (
                  <>
                    <div className="prep-section-head">
                      <p>Выберите тип данных для каждого признака и исключите лишние колонки.</p>
                      <button onClick={() => applyPreprocessingSection("types")} disabled={loading || !datasetFileId}>
                        {applyingSection === "types" ? "Применяем..." : "Применить"}
                      </button>
                    </div>
                    <div className="table-wrap compact flow-table">
                      <table className="types-table">
                        <thead>
                          <tr>
                            <th>Признак</th>
                            <th>Примеры</th>
                            <th>Тип данных</th>
                            <th>В названии</th>
                            <th>В сравнении</th>
                          </tr>
                        </thead>
                        <tbody>
                          {fields.map((field, index) => {
                            const column = preview?.columns.find((item) => item.normalized_name === field.key);
                            const fieldProfile = profile.find((item) => item.key === field.key);
                            const detectedDateFormat = fieldProfile?.recommended_config.datetime_format ?? "YYYY-MM-DD";
                            const detectedUnitFamily = fieldProfile?.detected_unit_family ?? field.unit_family ?? null;
                            const detectedUnits = fieldProfile?.detected_units ?? [];
                            const unitOptions = getUnitOptions(detectedUnitFamily);
                            const selectedTargetUnit = field.target_unit ?? fieldProfile?.target_unit ?? "";
                            return (
                              <tr key={field.key}>
                                <td>{field.key}</td>
                                <td className="types-table-samples">{column?.sample_values.slice(0, 3).map(String).join(", ") || "-"}</td>
                                <td>
                                  <div className="stacked-control">
                                    <div style={{ display: "flex", alignItems: "end", gap: "12px", flexWrap: "wrap" }}>
                                    <label className="sub-control" style={{ flex: "1 1 220px", minWidth: 0 }}>
                                      <span>Тип данных</span>
                                      <select
                                      value={field.field_type}
                                      onChange={(event) => {
                                        const nextType = event.target.value as FieldConfig["field_type"];
                                        updateField(index, {
                                          field_type: nextType,
                                          rounding_precision: nextType === "float" ? (field.rounding_precision ?? 2) : null,
                                          datetime_format: nextType === "datetime" ? (field.datetime_format ?? detectedDateFormat) : null,
                                        });
                                      }}
                                      >
                                        <option value="integer">{fieldTypeLabel("integer")}</option>
                                        <option value="float">{fieldTypeLabel("float")}</option>
                                        <option value="geo_latitude">{fieldTypeLabel("geo_latitude")}</option>
                                        <option value="geo_longitude">{fieldTypeLabel("geo_longitude")}</option>
                                        <option value="categorical">{fieldTypeLabel("categorical")}</option>
                                        <option value="binary">{fieldTypeLabel("binary")}</option>
                                        <option value="text">{fieldTypeLabel("text")}</option>
                                        <option value="datetime">{fieldTypeLabel("datetime")}</option>
                                      </select>
                                    </label>
                                    {field.field_type === "float" ? (
                                      <label className="sub-control" style={{ flex: "0 0 112px" }}>
                                        <span>Точность</span>
                                        <input
                                          type="number"
                                          min={0}
                                          max={10}
                                          step={1}
                                          value={field.rounding_precision ?? 2}
                                          onChange={(event) =>
                                            updateField(index, {
                                              rounding_precision: Math.max(0, Math.min(10, Number(event.target.value || 0))),
                                            })
                                          }
                                        />
                                      </label>
                                    ) : null}
                                    {isNumericFieldType(field.field_type) && unitOptions.length ? (
                                      <label className="sub-control" style={{ flex: "0 0 180px" }}>
                                        <span>Единицы</span>
                                        <select
                                          value={selectedTargetUnit}
                                          onChange={(event) =>
                                            updateField(index, {
                                              unit_family: detectedUnitFamily,
                                              target_unit: event.target.value || null,
                                            })
                                          }
                                        >
                                          {unitOptions.map((option) => (
                                            <option key={option.value} value={option.value}>{option.label}</option>
                                          ))}
                                        </select>
                                      </label>
                                    ) : null}
                                    {field.field_type === "datetime" ? (
                                      <label className="sub-control" style={{ flex: "0 0 180px" }}>
                                        <span>Формат даты</span>
                                        <select
                                          value={field.datetime_format ?? detectedDateFormat}
                                          onChange={(event) => updateField(index, { datetime_format: event.target.value })}
                                        >
                                          {DATETIME_FORMAT_OPTIONS.map((format) => (
                                            <option key={format} value={format}>{format}</option>
                                          ))}
                                        </select>
                                      </label>
                                    ) : null}
                                    </div>
                                    {field.field_type === "datetime" ? <small>Автоопределено: {detectedDateFormat}</small> : null}
                                    {isNumericFieldType(field.field_type) && detectedUnits.length ? (
                                      <small>Найдены единицы: {detectedUnits.join(", ")}</small>
                                    ) : null}
                                  </div>
                                </td>
                                <td>
                                  <label className="round-checkbox">
                                    <input
                                      type="checkbox"
                                      checked={Boolean(field.use_in_label)}
                                      onChange={(event) => updateField(index, { use_in_label: event.target.checked })}
                                    />
                                    <span>
                                      <i />
                                      <em>{field.use_in_label ? "Да" : "Нет"}</em>
                                    </span>
                                  </label>
                                </td>
                                <td>
                                  <label className="round-checkbox">
                                    <input
                                      type="checkbox"
                                      checked={field.include_in_output}
                                      onChange={(event) => updateField(index, { include_in_output: event.target.checked })}
                                    />
                                    <span>
                                      <i />
                                      <em>{field.include_in_output ? "Участвует" : "Исключен"}</em>
                                    </span>
                                  </label>
                                </td>
                              </tr>
                            );
                          })}
                        </tbody>
                      </table>
                    </div>
                  </>
                ) : null}

                {preprocessingSection === "encoding" ? (
                  <>
                    <div className="prep-section-head">
                      <p>Настройте кодирование категориальных признаков.</p>
                      <button onClick={() => applyPreprocessingSection("encoding")} disabled={loading || !datasetFileId}>
                        {applyingSection === "encoding" ? "Применяем..." : "Применить"}
                      </button>
                    </div>
                    <div className="field-board">
                      {activePreprocessingFields
                        .filter((field) => field.field_type === "categorical")
                        .map((field) => {
                          const index = fields.findIndex((item) => item.key === field.key);
                          const disabled = !field.include_in_output;
                          const column = encodingPreview?.columns.find((item) => item.normalized_name === field.key);
                          const categoryCounts = rawCategoryCountsForField(encodingPreview, field.key, column);
                          const categories = categoryCounts.map((item) => item.label);
                          const recommendedOrdinalMap = mergeOrdinalMap(categories, field.ordinal_map);
                          const samples = categoryCounts.slice(0, 10);
                          return (
                            <article className="field-card" key={`encoding-${field.key}`}>
                              <div className="field-card-head">
                                <div>
                                  <strong>{field.key}</strong>
                                  <span>{field.include_in_output ? "Участвует в анализе" : "Исключен из анализа"}</span>
                                </div>
                              </div>
                              {samples.length ? (
                                <div className="category-values">
                                  {samples.map((sample) => (
                                    <span key={`${field.key}-${sample.label}`}>
                                      {sample.label}
                                      {" "}
                                      <strong>{sample.value}</strong>
                                    </span>
                                  ))}
                                </div>
                              ) : null}
                              <div className="encoding-grid">
                                <LabelWithTip
                                  label="Ручная шкала категорий"
                                  tip="Система задает стартовые значения автоматически по формуле (позиция категории в списке / число категорий). Категории идут в текущем порядке списка, а вы можете вручную скорректировать любое значение."
                                />
                                <div className="manual-map-grid">
                                  {categories.map((category) => (
                                    <div key={`${field.key}-${category}`} className="manual-map-row">
                                      <span>{category}</span>
                                      <input
                                        type="number"
                                        step="0.01"
                                        disabled={disabled}
                                        value={String(field.ordinal_map?.[category] ?? recommendedOrdinalMap[category] ?? "")}
                                        onChange={(event) => {
                                          const raw = event.target.value;
                                          const currentMap = field.ordinal_map ?? recommendedOrdinalMap;
                                          const nextMap = { ...currentMap };
                                          if (raw === "") {
                                            nextMap[category] = recommendedOrdinalMap[category] ?? 0;
                                          } else {
                                            nextMap[category] = Number(raw);
                                          }
                                          updateField(index, { encoding: "ordinal", ordinal_map: nextMap });
                                        }}
                                      />
                                    </div>
                                  ))}
                                </div>
                              </div>
                            </article>
                          );
                        })}
                      {!activePreprocessingFields.some((field) => field.field_type === "categorical") ? (
                        <p className="muted-note">Категориальные колонки не найдены. Назначьте тип в разделе «Типы данных».</p>
                      ) : null}
                    </div>
                  </>
                ) : null}

                {preprocessingSection === "scaling" ? (
                  <>
                    <div className="prep-section-head">
                      <p>Выберите способ масштабирования для числовых признаков.</p>
                      <button onClick={() => applyPreprocessingSection("scaling")} disabled={loading || !datasetFileId}>
                        {applyingSection === "scaling" ? "Применяем..." : "Применить"}
                      </button>
                    </div>
                    <div className="field-board">
                      {fields
                        .filter((field) => field.include_in_output && isNumericFieldType(field.field_type))
                        .map((field) => {
                          const index = fields.findIndex((item) => item.key === field.key);
                          const disabled = !field.include_in_output;
                          return (
                            <article className="field-card" key={`scaling-${field.key}`}>
                              <div className="field-card-head">
                                <div>
                                  <strong>{field.key}</strong>
                                  <span>{disabled ? "Исключен из анализа" : "Участвует в анализе"}</span>
                                </div>
                              </div>
                              <div className="control-grid">
                                <label>
                                  <LabelWithTip
                                    label="Масштабирование"
                                    tip="Нормализация числовых признаков к сопоставимому масштабу, чтобы исключить непропорциональное влияние признаков с крупными абсолютными значениями."
                                  />
                                  <select
                                    value={field.normalization}
                                    disabled={disabled}
                                    onChange={(event) => updateFieldNormalization(index, event.target.value as FieldConfig["normalization"])}
                                  >
                                    <option value="minmax">{methodLabel("minmax")}</option>
                                    <option value="zscore">{methodLabel("zscore")}</option>
                                    <option value="robust">{methodLabel("robust")}</option>
                                    <option value="log_minmax">{methodLabel("log_minmax")}</option>
                                  </select>
                                </label>
                              </div>
                            </article>
                          );
                        })}
                      {!activePreprocessingFields.some((field) => isNumericFieldType(field.field_type)) ? (
                        <p className="muted-note">Числовые колонки не найдены. Назначьте тип в разделе «Типы данных».</p>
                      ) : null}
                    </div>
                  </>
                ) : null}

                {preprocessingSection === "missing" ? (
                  <>
                    <div className="prep-section-head">
                      <p>Задайте стратегию обработки пропусков для нужных полей.</p>
                      <button onClick={() => applyPreprocessingSection("missing")} disabled={loading || !datasetFileId}>
                        {applyingSection === "missing" ? "Применяем..." : "Применить"}
                      </button>
                    </div>
                    <div className="table-wrap dataset-table wide">
                      <table>
                        <thead>
                          <tr>
                            <th>ID</th>
                            {visiblePreviewColumns.map((column) => (
                              <th key={`missing-head-${column.normalized_name}`}>{column.normalized_name}</th>
                            ))}
                          </tr>
                        </thead>
                        <tbody>
                          {missingRowsPreview.map((row) => {
                            return (
                              <tr key={row.id}>
                                <td>{row.id}</td>
                                {visiblePreviewColumns.map((column) => {
                                  const value = row.values[column.normalized_name];
                                  const isMissing = value === null || value === undefined || value === "";
                                  return (
                                    <td key={`${row.id}-${column.normalized_name}`}>
                                      {isMissing ? <span className="muted-cell">Пусто</span> : formatPreviewCell(value)}
                                    </td>
                                  );
                                })}
                              </tr>
                            );
                          })}
                        </tbody>
                      </table>
                    </div>
                    {!missingRowsPreview.length ? <p className="muted-note">Строк с пропусками в текущем предпросмотре нет.</p> : null}

                    <div className="field-board">
                      {fields
                        .filter(
                          (field) =>
                            field.include_in_output
                            && (preview?.columns.find((column) => column.normalized_name === field.key)?.missing_count ?? 0) > 0,
                        )
                        .map((field) => {
                          const index = fields.findIndex((item) => item.key === field.key);
                          const missingCount = preview?.columns.find((column) => column.normalized_name === field.key)?.missing_count ?? 0;
                          return (
                            <article className="field-card" key={field.key}>
                              <div className="field-card-head">
                                <div>
                                  <strong>{field.key}</strong>
                                  <span>{missingCount} пропусков</span>
                                </div>
                              </div>
                              <div className="control-grid">
                                <label>
                                  <LabelWithTip
                                    label="Что делать с пропусками"
                                    tip="Политика обработки отсутствующих значений: исключение наблюдений, статистическая импутация или подстановка фиксированной константы."
                                  />
                                  <select
                                    value={field.missing_strategy}
                                    onChange={(event) => {
                                      const nextStrategy = event.target.value;
                                      const needsDefaultConstant =
                                        nextStrategy === "constant"
                                        && (field.missing_constant === null || field.missing_constant === undefined || field.missing_constant === "");
                                      updateField(
                                        index,
                                        needsDefaultConstant
                                          ? {
                                            missing_strategy: nextStrategy,
                                            missing_constant: isNumericFieldType(field.field_type) ? 0 : "unknown",
                                          }
                                          : { missing_strategy: nextStrategy },
                                      );
                                    }}
                                  >
                                    <option value="none">{methodLabel("none")}</option>
                                    <option value="drop_row">{methodLabel("drop_row")}</option>
                                    {isNumericFieldType(field.field_type) ? <option value="median">{methodLabel("median")}</option> : null}
                                    {isNumericFieldType(field.field_type) ? <option value="mean">{methodLabel("mean")}</option> : null}
                                    <option value="mode">{methodLabel("mode")}</option>
                                    <option value="constant">{methodLabel("constant")}</option>
                                  </select>
                                </label>
                                {field.missing_strategy === "constant" ? (
                                  <label>
                                    Подставляемое значение
                                    <input
                                      type={isNumericFieldType(field.field_type) ? "number" : "text"}
                                      step={isNumericFieldType(field.field_type) ? "any" : undefined}
                                      value={String(field.missing_constant ?? "")}
                                      onChange={(event) => {
                                        const rawValue = event.target.value;
                                        updateField(index, {
                                          missing_constant: isNumericFieldType(field.field_type)
                                            ? (rawValue === "" ? null : Number(rawValue))
                                            : rawValue,
                                        });
                                      }}
                                      placeholder="например 0 или unknown"
                                    />
                                  </label>
                                ) : null}
                              </div>
                            </article>
                          );
                        })}
                    </div>
                  </>
                ) : null}

                {preprocessingSection === "outliers" ? (
                  <>
                    <div className="prep-section-head">
                      <p>Настройте правила обработки выбросов для числовых признаков.</p>
                      <button onClick={() => applyPreprocessingSection("outliers")} disabled={loading || !datasetFileId}>
                        {applyingSection === "outliers" ? "Применяем..." : "Применить"}
                      </button>
                    </div>
                    <div className="field-board">
                      {fields
                        .filter((field) => field.include_in_output && isNumericFieldType(field.field_type))
                        .map((field) => {
                          const index = fields.findIndex((item) => item.key === field.key);
                          const fieldProfile = profile.find((item) => item.key === field.key);
                          const values = numericValuesForField(preview, field.key);
                          const currentMode = chartModeByField[field.key] ?? "histogram";
                          const appliedBins = histogramBinsByField[field.key] ?? 8;
                          const outliersCount = fieldProfile?.outlier_count_iqr ?? 0;
                          return (
                            <article className="field-card" key={field.key}>
                              <div className="field-card-head">
                                <div>
                                  <strong>{field.key}</strong>
                                  <span>Найдено выбросов: {outliersCount}</span>
                                </div>
                                <div className="chart-mode-switch">
                                  <button
                                    className={`ghost-button compact ${currentMode === "histogram" ? "active" : ""}`}
                                    type="button"
                                    onClick={() => setChartModeByField((current) => ({ ...current, [field.key]: "histogram" }))}
                                  >
                                    Гистограмма
                                  </button>
                                  <button
                                    className={`ghost-button compact ${currentMode === "boxplot" ? "active" : ""}`}
                                    type="button"
                                    onClick={() => setChartModeByField((current) => ({ ...current, [field.key]: "boxplot" }))}
                                  >
                                    Ящик с усами
                                  </button>
                                </div>
                              </div>
                              <DistributionChart
                                title={`Распределение ${field.key}`}
                                values={values}
                                bins={appliedBins}
                                mode={currentMode}
                                histogramData={fieldProfile?.histogram ?? []}
                                boxplotData={fieldProfile?.boxplot_stats ?? null}
                              />
                              <div className="control-grid">
                                {currentMode === "histogram" ? (
                                  <label>
                                    <LabelWithTip
                                      label="Столбцов гистограммы"
                                      tip="Число интервалов разбиения признака для оценки распределения. Увеличение числа интервалов повышает детализацию и чувствительность к шуму."
                                    />
                                    <input
                                      type="number"
                                      min={2}
                                      max={64}
                                      step="1"
                                      value={histogramDraftBinsByField[field.key] ?? histogramBinsByField[field.key] ?? 8}
                                      onChange={(event) =>
                                        setHistogramDraftBinsByField((current) => ({
                                          ...current,
                                          [field.key]: Math.min(64, Math.max(2, Number(event.target.value) || 8)),
                                        }))
                                      }
                                    />
                                  </label>
                                ) : null}
                                <button className="chart-redraw-button" type="button" onClick={() => applyHistogramBins(field.key)} disabled={loading}>
                                  Перерисовать график
                                </button>
                                <label>
                                  <LabelWithTip
                                    label="Что делать с выбросами"
                                    tip="Стратегия обработки экстремальных наблюдений: ограничение значений до порогов либо исключение соответствующих записей."
                                  />
                                  <select value={field.outlier_method} onChange={(event) => updateField(index, { outlier_method: event.target.value })}>
                                    <option value="none">{methodLabel("none")}</option>
                                    <option value="iqr_clip">{methodLabel("iqr_clip")}</option>
                                    <option value="iqr_remove">{methodLabel("iqr_remove")}</option>
                                    <option value="zscore_clip">{methodLabel("zscore_clip")}</option>
                                    <option value="zscore_remove">{methodLabel("zscore_remove")}</option>
                                  </select>
                                </label>
                                <label>
                                  <LabelWithTip
                                    label="Порог"
                                    tip="Параметр чувствительности правила обнаружения выбросов; снижение порога увеличивает число наблюдений, классифицируемых как выбросы."
                                  />
                                  <input
                                    type="number"
                                    min={0}
                                    step="0.1"
                                    value={field.outlier_threshold}
                                    onChange={(event) => updateField(index, { outlier_threshold: Number(event.target.value) || 1.5 })}
                                  />
                                </label>
                              </div>
                            </article>
                          );
                        })}
                      {!fields.some((field) => isNumericFieldType(field.field_type)) ? (
                        <p className="muted-note">В текущей конфигурации нет числовых колонок для построения гистограмм.</p>
                      ) : null}
                    </div>
                  </>
                ) : null}
              </>
            ) : datasetFileId && sourceFilename ? (
              <div className="empty-state">Загружаем настройки подготовки...</div>
            ) : (
              <div className="empty-state">Сначала загрузите файл.</div>
            )}
          </section>
        ) : null}

        {activeStage === "criteria" ? (
          <section className="panel">
            <div className="panel-head">
              <div>
                <span className="section-kicker">Этап 3</span>
                <h2>Режим и веса критериев</h2>
              </div>
              {criteria.length > 0 ? (
                <div className="criteria-actions">
                  <div className="weight-meter">
                    <span><LabelWithTip label="Σ весов" tip={<><span>Сумма весов критериев в нормализованной постановке:</span><Formula latex={"\\sum_{i=1}^{n} w_i = 1"} /></>} /></span>
                    <strong>{totalWeight.toFixed(3)}</strong>
                  </div>
                  <button className="ghost-button" onClick={setEqualWeights} disabled={criteria.length === 0}>
                    Поставить одинаковые веса
                  </button>
                  <button className="ghost-button" onClick={applyRecommendedWeights} disabled={!Object.keys(recommendedWeights).length}>
                    Рекомендованные веса
                  </button>
                  <button onClick={normalizeWeights} disabled={criteria.length === 0 || totalWeight <= 0}>
                    Нормализовать веса
                  </button>
                </div>
              ) : null}
            </div>

            <div className="target-panel focus-target-panel">
              <label>
                <LabelWithTip
                  label="Режим анализа"
                  tip="Определяет постановку задачи: глобальное ранжирование всей выборки либо оценка близости объектов к выбранному эталону."
                />
                <select value={analysisMode} onChange={(event) => updateAnalysisMode(event.target.value as AnalysisMode)}>
                  <option value="analog_search">Поиск аналогов для целевого объекта</option>
                  <option value="rating">Общий рейтинг объектов</option>
                </select>
              </label>
              {analysisMode === "analog_search" ? (
                <>
                  <label>
                    Целевой объект
                    <select value={targetRowId} onChange={(event) => setTargetRowId(event.target.value)}>
                      {(preview?.normalized_dataset?.rows ?? []).map((row) => {
                        const label = objectLabelMap.get(row.id) || `Объект ${row.id}`;
                        return <option key={row.id} value={row.id}>{row.id} · {label}</option>;
                      })}
                    </select>
                  </label>
                  {geoSearchAvailable ? (
                    <label>
                      Радиус, км
                      <input
                        type="number"
                        min="0"
                        step="0.1"
                        value={geoRadiusKm ?? ""}
                        onChange={(event) => setGeoRadiusKm(event.target.value === "" ? null : Math.max(0, Number(event.target.value)))}
                        placeholder="Без ограничения"
                      />
                    </label>
                  ) : null}
                  {selectedTargetRow ? (
                    <TargetObjectPreviewCard
                      objectId={selectedTargetRow.id}
                      title={objectLabelMap.get(selectedTargetRow.id) || `Объект ${selectedTargetRow.id}`}
                      items={selectedTargetPreviewItems}
                    />
                  ) : null}
                </>
              ) : (
                <div className="target-explain">Целевой объект не требуется.</div>
              )}
              <div className="target-explain">
                {analysisMode === "analog_search"
                  ? geoSearchAvailable
                    ? "Выберите целевой объект и при необходимости ограничьте радиус поиска."
                    : "Выберите целевой объект и настройте критерии."
                  : "Настройте критерии и веса для построения рейтинга."}
              </div>
            </div>
            {weightNotes.length > 0 ? (
              <div className="weight-notes">
                {weightNotes.map((note) => (
                  <p key={note}>{note}</p>
                ))}
              </div>
            ) : null}
            {criteria.length > 0 ? (
              <div className="criteria-board">
                {criteria.map((criterion, index) => (
                  <article className="criterion-card" key={criterion.key}>
                    <div className="criterion-main">
                      <h3>{criterion.key.toUpperCase()}</h3>
                    </div>
                    <div className="slider-block">
                      <input
                        type="range"
                        min="0"
                        max="1"
                        step="0.01"
                        value={criterion.weight}
                        onChange={(event) => updateCriterion(index, { weight: Number(event.target.value) })}
                      />
                      <input
                        type="number"
                        min="0"
                        step="0.01"
                        value={criterion.weight}
                        onChange={(event) => updateCriterion(index, { weight: Number(event.target.value) })}
                      />
                    </div>
                    <label>
                      <LabelWithTip
                        label="Направление"
                        tip="Правило монотонности критерия: максимизация, минимизация либо минимизация отклонения от целевого значения."
                      />
                      <select
                        value={criterion.direction}
                        onChange={(event) => updateCriterion(index, { direction: event.target.value as CriterionConfig["direction"] })}
                      >
                        <option value="maximize">Больше лучше</option>
                        <option value="minimize">Меньше лучше</option>
                        <option value="target">Ближе к цели</option>
                      </select>
                    </label>
                  </article>
                ))}
              </div>
            ) : (
              <p className="muted-note">Критерии пока не сформированы. Вернитесь на этап 2 и нажмите «Сформировать критерии».</p>
            )}
          </section>
        ) : null}

        {activeStage === "results" ? (
          <section className="results-layout">
            <div className="panel">
              <div className="panel-head">
                <div>
                  <span className="section-kicker">Этап 4</span>
                  <h2>{analysisMode === "analog_search" ? "Найденные аналоги" : "Результаты расчета"}</h2>
                  <p>
                    {analysisMode === "analog_search"
                      ? "Ближайшие объекты к выбранному целевому объекту."
                      : "Итоговый рейтинг, объяснение результата и экспорт отчета сравнения."}
                  </p>
                </div>
                <div className="report-actions">
                  <button className="ghost-button" onClick={openReportPreview} disabled={!result}>Предпросмотр</button>
                  <button className="ghost-button" onClick={exportSortedDatasetCsv} disabled={!result}>Скачать отсортированный датасет</button>
                  <button className="ghost-button" onClick={exportJsonReport} disabled={!result}>JSON</button>
                  <button className="ghost-button" onClick={exportHtmlReport} disabled={!result}>HTML</button>
                  <button className="ghost-button" onClick={exportPdfReport} disabled={!result || loading}>PDF</button>
                  <button onClick={exportDocxReport} disabled={!result || loading}>DOCX</button>
                </div>
              </div>

               {displayResult ? (() => {
                 const objectLabelById = new Map(displayResult.ranking.map((item) => [String(item.object_id), item.title || `Объект ${item.object_id}`]));
                 return (
                 <>
                  <div className="metric-row">
                    {summaryCardEntries(displayResult.analysis_summary).filter(([key]) => key !== "weights_sum").map(([key, value]) => (
                      <div className="metric" key={key}>
                        <span>{summaryHelpText(key) ? <LabelWithTip label={labelForSummaryKey(key)} tip={summaryHelpText(key)} /> : labelForSummaryKey(key)}</span>
                        <strong>{formatSummaryValue(key, value)}</strong>
                      </div>
                    ))}
                  </div>
                  <div className="insight-grid">
                    <div className="insight-card">
                      <span className="section-kicker">Надежность</span>
                      <h3>Доверие и устойчивость</h3>
                      <p>{translateAnalysisText(displayResult.analysis_summary.ranking_stability_note ?? "Нет данных об устойчивости рейтинга.")}</p>
                      {summaryList<RankingStabilityScenario>(displayResult.analysis_summary, "ranking_stability_scenarios").map((scenario) => (
                        <div className="stability-row" key={scenario.label}>
                          <strong>{scenario.label}</strong>
                           <span>Лидер: {scenario.top_object_id ? objectLabelById.get(String(scenario.top_object_id)) || `№${scenario.top_object_id}` : "-"}</span>
                          <span>Совпадение топ-N: {scenario.top_n_overlap}</span>
                          <small>{translateAnalysisText(scenario.note)}</small>
                        </div>
                      ))}
                      {summaryList<string>(displayResult.analysis_summary, "confidence_notes").map((item, index) => (
                        <p key={`${item}-${index}`}>{translateAnalysisText(item)}</p>
                      ))}
                    </div>
                    <div className="insight-card">
                      <span className="section-kicker">Аналоги</span>
                      <h3>Группы аналогов</h3>
                      {summaryList<{ label: string; object_ids: string[] }>(displayResult.analysis_summary, "analog_groups").length ? (
                        summaryList<{ label: string; object_ids: string[] }>(displayResult.analysis_summary, "analog_groups").map((group) => (
                          <div className="group-chip" key={group.label}>
                            <strong>{translateAnalysisText(group.label)}</strong>
                            <span>{group.object_ids.length} Объектов</span>
                          </div>
                        ))
                      ) : (
                        <p>Группы аналогов доступны в режиме поиска аналогов.</p>
                      )}
                    </div>
                  </div>
                  <div className="analytics-grid">
                    <ResultBarChart
                      result={displayResult}
                      objectLabelMap={objectLabelMap}
                      mode={analysisMode}
                      selectedId={selectedDisplayResult?.object_id ?? ""}
                      onSelect={setSelectedResultId}
                      currentPage={resultsPage}
                      onPageChange={changeResultsPage}
                    />
                    <RadarComparisonChart
                      item={selectedDisplayResult}
                      baseline={displayResult.ranking[0]}
                    />
                    <ContributionWaterfallChart
                      item={selectedDisplayResult}
                      mode={analysisMode}
                    />
                  </div>
                  <ObjectDetailsPanel
                    item={selectedDisplayResult}
                    objectLabelMap={objectLabelMap}
                    result={displayResult}
                    mode={analysisMode}
                    rawValues={selectedDisplayResult ? rawResultValuesById[selectedDisplayResult.object_id] ?? null : null}
                  />
                 </>
                 );
               })() : (
                <div className="empty-state">
                  Расчет еще не выполнен. Перейдите к критериям и запустите анализ.
                </div>
              )}
            </div>
          </section>
        ) : null}

        {activeStage === "projects" ? (
          <section className="panel">
            <div className="panel-head">
              <div>
                <span className="section-kicker">Рабочая область</span>
                <h2>Проекты анализа</h2>
                <p>Проект объединяет датасеты, версии сценариев, результаты и отчеты. Это помогает не терять контекст между повторными расчетами.</p>
              </div>
              <button onClick={() => refreshHistory(false)} disabled={!user || loading}>Обновить</button>
            </div>
            {user ? (
              <>
                <div className="project-create-panel">
                  <label>
                    Название проекта
                    <input value={newProjectName} onChange={(event) => setNewProjectName(event.target.value)} />
                  </label>
                  <button onClick={handleCreateProject} disabled={loading || !newProjectName.trim()}>
                    Создать проект
                  </button>
                </div>
                {projects.length > 0 ? (
                  <div className="project-grid">
                    {projects.map((project) => {
                      const projectHistory = history.filter((item) => item.project_id === project.id);
                      const latest = projectHistory[0];
                      const latestSummary = parseHistorySummary(latest);
                      return (
                        <article className="project-card" key={project.id}>
                          <span className="tag">{translateStatus(project.status)}</span>
                          <h3>{project.name}</h3>
                          <p>{project.description || "Рабочее пространство сравнительного анализа"}</p>
                          <div className="project-stats">
                            <div><span>Сценариев</span><strong>{projectHistory.length}</strong></div>
                            <div><span>Последняя версия</span><strong>{latest ? `v${latest.version_number}` : "-"}</strong></div>
                            <div><span>Лучшая оценка</span><strong>{latest ? String(latestSummary.analysis_summary?.best_score ?? "-") : "-"}</strong></div>
                          </div>
                          <div className="project-actions">
                            <button
                              className="ghost-button"
                              onClick={() => {
                                setActiveProjectId(project.id);
                                setActiveStage("criteria");
                              }}
                            >
                              Использовать
                            </button>
                            <button
                              onClick={() => {
                                setHistoryProjectFilter(project.id);
                                setActiveStage("history");
                              }}
                            >
                              История
                            </button>
                          </div>
                        </article>
                      );
                    })}
                  </div>
                ) : (
                  <div className="empty-state">Проектов пока нет. Создайте первый проект, чтобы группировать сценарии анализа.</div>
                )}
              </>
            ) : (
              <div className="empty-state">Войдите в систему, чтобы работать с проектами.</div>
            )}
          </section>
        ) : null}

        {activeStage === "history" ? (
          <section className="panel">
            <div className="panel-head">
              <div>
                <span className="section-kicker">Личный кабинет</span>
                <h2>История сравнений</h2>
                <p>Здесь сохраняются параметры расчетов и итоговые результаты авторизованного пользователя.</p>
              </div>
              <button onClick={() => refreshHistory()} disabled={!user || loading}>Обновить</button>
            </div>
            {user ? (
              history.length > 0 ? (
                <>
                <div className="history-toolbar">
                  <label>
                    Проект
                    <select
                      value={historyProjectFilter}
                      onChange={(event) => setHistoryProjectFilter(event.target.value === "all" ? "all" : Number(event.target.value))}
                    >
                      <option value="all">Все проекты</option>
                      {projects.map((project) => (
                        <option key={project.id} value={project.id}>{project.name}</option>
                      ))}
                    </select>
                  </label>
                  <div className="history-toolbar-note">
                    Выберите два сценария, чтобы сравнить версии расчета.
                  </div>
                </div>

                {comparedHistory.length === 2 ? (
                  <div className="compare-panel">
                    {comparedHistory.map((item) => {
                      const summary = parseHistorySummary(item);
                      return (
                        <div className="compare-card" key={item.id}>
                          <span className="tag">v{item.version_number}</span>
                          <h3>{item.title}</h3>
                          <div className="metric mini">
                            <span>Лучший объект</span>
                            <strong>{String(summary.analysis_summary?.best_object_id ?? "-")}</strong>
                          </div>
                          <div className="metric mini">
                            <span>Оценка</span>
                            <strong>{summaryNumber(summary, "best_score").toFixed(4)}</strong>
                          </div>
                          <div className="metric mini">
                            <span><LabelWithTip label="Confidence" tip={<><span>Оценка статистической устойчивости результата:</span><Formula latex={"C = 1 - (aM + bO + cS)"} /></>} /></span>
                            <strong>{summaryNumber(summary, "confidence_score").toFixed(4)}</strong>
                          </div>
                        </div>
                      );
                    })}
                    <div className="compare-delta">
                      <span><LabelWithTip label="Δ лучшей оценки" tip={<><span>Разность максимальных интегральных оценок двух сценариев:</span><Formula latex={"\\Delta = best\\_score_{new} - best\\_score_{old}"} /></>} /></span>
                      <strong>
                        {(summaryNumber(parseHistorySummary(comparedHistory[1]), "best_score") -
                          summaryNumber(parseHistorySummary(comparedHistory[0]), "best_score")).toFixed(4)}
                      </strong>
                    </div>
                  </div>
                ) : null}

                <div className="history-list">
                  {filteredHistory.map((item) => {
                    const summary = parseHistorySummary(item);
                    return (
                      <article className="history-card" key={item.id}>
                        <div>
                          <span className="tag">{translateStatus(item.status)}</span>
                          <h3>{item.title}</h3>
                          <p>
                            {new Date(item.created_at).toLocaleString("ru-RU")} · {item.source_filename}
                            {" "}· v{item.version_number}
                            {item.project_id ? ` · проект №${item.project_id}` : ""}
                          </p>
                        </div>
                        <div className="metric mini">
                          <span>Файлы</span>
                          <strong>{item.dataset_file_id && item.result_file_id ? "Файловое Хранилище" : "Только БД"}</strong>
                        </div>
                        <div className="metric mini">
                          <span>Лучший объект</span>
                          <strong>{String(summary.analysis_summary?.best_object_id ?? "-")}</strong>
                        </div>
                        <div className="metric mini">
                          <span>Лучшая оценка</span>
                          <strong>{String(summary.analysis_summary?.best_score ?? "-")}</strong>
                        </div>
                        <button
                          className={historyCompareIds.includes(item.id) ? "compare-toggle active" : "compare-toggle"}
                          onClick={() => toggleHistoryCompare(item.id)}
                        >
                          {historyCompareIds.includes(item.id) ? "Выбрано" : "Сравнить"}
                        </button>
                        <button className="ghost-button compact" onClick={() => applyHistoryScenario(item)}>
                          Повторить
                        </button>
                      </article>
                    );
                  })}
                </div>
                </>
              ) : (
                <div className="empty-state">История пока пуста. Запустите расчет после входа в систему.</div>
              )
            ) : (
              <div className="empty-state">Войдите в систему, чтобы сохранять и просматривать историю.</div>
            )}
          </section>
        ) : null}

        {activeStage === "admin" ? (
          <section className="panel">
            <div className="panel-head">
              <div>
                <span className="section-kicker">Административный кабинет</span>
                <h2>Дэшборд системы</h2>
                <p>Обзор пользователей и базовых показателей платформы.</p>
              </div>
              <button onClick={refreshAdminDashboard} disabled={user?.role !== "admin" || loading}>Обновить</button>
            </div>
            {user?.role === "admin" ? (
              <>
                <div className="metric-row">
                  <div className="metric"><span>Пользователей</span><strong>{adminStats?.users_total ?? "-"}</strong></div>
                  <div className="metric"><span>Администраторов</span><strong>{adminStats?.admins_total ?? "-"}</strong></div>
                  <div className="metric"><span>Активных</span><strong>{adminStats?.active_users_total ?? "-"}</strong></div>
                  <div className="metric"><span>Проектов</span><strong>{systemDashboard?.storage?.projects_total ?? "-"}</strong></div>
                  <div className="metric"><span>Сравнений</span><strong>{systemDashboard?.storage?.comparisons_total ?? "-"}</strong></div>
                  <div className="metric"><span>Файлов</span><strong>{systemDashboard?.storage?.files_total ?? "-"}</strong></div>
                </div>
                {systemDashboard ? (
                  <div className="service-grid">
                    {Object.entries(systemDashboard.services).map(([name, service]) => (
                      <div className="service-card" key={name}>
                        <span className={service.status === "ok" ? "health-dot ok" : "health-dot"} />
                        <strong>{SERVICE_LABELS[name] ?? name}</strong>
                        <small>{translateStatus(service.status)}{service.status_code ? ` · ${service.status_code}` : ""}</small>
                      </div>
                    ))}
                  </div>
                ) : null}
                {systemDashboard?.telemetry ? (
                  <>
                    <div className="metric-row">
                      <div className="metric"><span>Запросов (окно)</span><strong>{systemDashboard.telemetry.overall.requests}</strong></div>
                      <div className="metric"><span>Ошибок (окно)</span><strong>{systemDashboard.telemetry.overall.errors}</strong></div>
                      <div className="metric"><span>Средний отклик, мс</span><strong>{systemDashboard.telemetry.overall.avg_ms.toFixed(2)}</strong></div>
                      <div className="metric"><span>Ошибка, %</span><strong>{systemDashboard.telemetry.overall.error_rate_pct.toFixed(2)}</strong></div>
                    </div>
                    <div className="panel subtle-panel">
                      <div className="panel-head compact-head">
                        <div>
                          <span className="section-kicker">Производительность</span>
                          <h3>Средний отклик по модулям</h3>
                          <p>Легкая сводка по latency и ошибкам, без перегруза дашборда.</p>
                        </div>
                      </div>
                      <div className="bar-list">
                        {(() => {
                          const modules = systemDashboard.telemetry?.modules ?? [];
                          const maxAvg = Math.max(...modules.map((item) => item.avg_ms), 1);
                          return modules.slice(0, 6).map((item) => (
                            <div className="bar-item" key={item.module}>
                              <span title={SERVICE_LABELS[item.module] ?? item.module}>{SERVICE_LABELS[item.module] ?? item.module}</span>
                              <div className="bar-track" title={`avg ${item.avg_ms.toFixed(2)} ms, p95 ${item.p95_ms.toFixed(2)} ms`}>
                                <div className="bar-fill" style={{ width: `${Math.max(4, (item.avg_ms / maxAvg) * 100)}%` }} />
                              </div>
                              <strong>{item.avg_ms.toFixed(1)} ms</strong>
                            </div>
                          ));
                        })()}
                      </div>
                    </div>
                  </>
                ) : null}
                <div className="table-wrap">
                  <table>
                    <thead>
                      <tr><th>ID</th><th>Email</th><th>Имя</th><th>Роль</th><th>Статус</th></tr>
                    </thead>
                    <tbody>
                      {adminUsers.map((item) => (
                        <tr key={item.id}>
                          <td>{item.id}</td>
                          <td>{item.email}</td>
                          <td>{item.full_name}</td>
                          <td><span className="tag">{item.role}</span></td>
                          <td>{item.is_active ? "Активен" : "Заблокирован"}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </>
            ) : (
              <div className="empty-state">Этот раздел доступен только администраторам.</div>
            )}
          </section>
        ) : null}

        {reportPreviewOpen && result ? (
          <div className="modal-backdrop report-backdrop" role="presentation" onMouseDown={() => setReportPreviewOpen(false)}>
            <section className="report-modal" role="dialog" aria-modal="true" onMouseDown={(event) => event.stopPropagation()}>
              <div className="report-modal-head">
                <div>
                  <span className="section-kicker">Предпросмотр</span>
                  <h2>Предпросмотр отчета</h2>
                  <p>Проверьте состав отчета перед скачиванием: сводка, критерии, рейтинг и объяснение результата.</p>
                </div>
                <button className="modal-close" onClick={() => setReportPreviewOpen(false)} aria-label="Закрыть">×</button>
              </div>
              <div className="report-preview-toolbar">
                <button className="ghost-button" onClick={exportSortedDatasetCsv}>Скачать отсортированный датасет</button>
                <button className="ghost-button" onClick={exportJsonReport}>Скачать JSON</button>
                <button className="ghost-button" onClick={exportHtmlReport}>Скачать HTML</button>
                <button onClick={exportDocxReport} disabled={loading}>Скачать DOCX</button>
              </div>
              <iframe className="report-preview-frame" title="Предпросмотр отчета" srcDoc={reportPreviewHtml} />
            </section>
          </div>
        ) : null}

        {authModalOpen ? (
          <div className="modal-backdrop" role="presentation" onMouseDown={() => {
            setAuthError(null);
            setAuthModalOpen(false);
          }}>
            <section className="auth-modal" role="dialog" aria-modal="true" onMouseDown={(event) => event.stopPropagation()}>
              <button className="modal-close" onClick={() => {
                setAuthError(null);
                setAuthModalOpen(false);
              }} aria-label="Закрыть">×</button>
              <span className="section-kicker">{authMode === "login" ? "Вход в систему" : "Новый аккаунт"}</span>
              <h2>{authMode === "login" ? "Добро пожаловать" : "Регистрация"}</h2>
              <p>
                {authMode === "login"
                  ? "Войдите, чтобы сохранять историю сравнений и получать доступ к личному кабинету."
                  : "Создайте аккаунт, чтобы сохранять параметры расчетов и возвращаться к результатам позже."}
              </p>
              <div className="auth-form">
                {authError ? <div className="auth-error" role="alert">{authError}</div> : null}
                {authMode === "register" ? (
                  <label>
                    Имя
                    <input value={authName} onChange={(event) => {
                      setAuthName(event.target.value);
                      setAuthError(null);
                    }} placeholder="Иван Иванов" />
                  </label>
                ) : null}
                <label>
                  Email
                  <input value={authEmail} onChange={(event) => {
                    setAuthEmail(event.target.value);
                    setAuthError(null);
                  }} placeholder="name@example.com" />
                </label>
                <label>
                  Пароль
                  <input
                    type="password"
                    value={authPassword}
                    onChange={(event) => {
                      setAuthPassword(event.target.value);
                      setAuthError(null);
                    }}
                    placeholder="Минимум 6 символов"
                    onKeyDown={(event) => {
                      if (event.key === "Enter") {
                        void handleAuthSubmit();
                      }
                    }}
                  />
                </label>
                <button className="wide-button" onClick={handleAuthSubmit} disabled={loading}>
                  {loading ? "Проверяем..." : authMode === "login" ? "Войти" : "Зарегистрироваться"}
                </button>
              </div>
              <div className="auth-switch">
                <span>{authMode === "login" ? "Еще нет аккаунта?" : "Уже зарегистрированы?"}</span>
                <button
                  className="link-button"
                  onClick={() => {
                    setAuthError(null);
                    setAuthMode(authMode === "login" ? "register" : "login");
                  }}
                >
                  {authMode === "login" ? "Зарегистрироваться" : "Войти"}
                </button>
              </div>
              {authMode === "login" ? (
                <div className="demo-hint">
                  <strong>Демо-админ:</strong> admin@example.com / admin12345
                </div>
              ) : null}
            </section>
          </div>
        ) : null}
      </main>
    </div>
  );
}
