import { useEffect, useMemo, useState } from "react";
import {
  createProject,
  downloadDocxReport,
  fetchAdminStats,
  fetchAdminUsers,
  fetchHistory,
  fetchMe,
  fetchProjects,
  fetchSystemDashboard,
  login,
  profileFile,
  register,
  runPipeline,
} from "./api";
import type {
  AdminStats,
  AnalysisMode,
  AuthUser,
  ComparisonHistoryItem,
  CriterionConfig,
  FieldProfile,
  FieldConfig,
  PipelineResult,
  PreviewColumn,
  PreviewResponse,
  DatasetQualityReport,
  ProjectItem,
  SystemDashboard,
} from "./types";

type StageId = "data" | "preprocessing" | "criteria" | "results" | "projects" | "history" | "admin";

const stages: Array<{ id: StageId; title: string; caption: string }> = [
  { id: "data", title: "Данные", caption: "Загрузка и предпросмотр" },
  { id: "preprocessing", title: "Подготовка", caption: "Очистка и кодирование" },
  { id: "criteria", title: "Критерии", caption: "Веса и направления" },
  { id: "results", title: "Результаты", caption: "Рейтинг и отчет" },
  { id: "admin", title: "Админ", caption: "Дэшборд системы" },
];

const numericNameHints = ["price", "cost", "area", "rating", "score", "days", "months", "amount", "value"];
const minimizeHints = ["price", "cost", "distance", "time", "days", "risk", "loss"];

function looksNumeric(column: PreviewColumn) {
  const samples = column.sample_values.filter((value) => value !== null && value !== "");
  if (samples.length === 0) return column.inferred_type === "numeric";
  return samples.every((value) => !Number.isNaN(Number(String(value).replace(",", "."))));
}

function inferUiType(column: PreviewColumn): FieldConfig["field_type"] {
  if (looksNumeric(column) || numericNameHints.some((hint) => column.normalized_name.includes(hint))) return "numeric";
  return column.inferred_type;
}

function uniqueSamples(column: PreviewColumn) {
  return Array.from(new Set(column.sample_values.filter((value) => value !== null && value !== "").map(String)));
}

function fieldDefaults(column: PreviewColumn): FieldConfig {
  const fieldType = inferUiType(column);
  const categories = uniqueSamples(column);
  return {
    key: column.normalized_name,
    field_type: fieldType,
    include_in_output: true,
    missing_strategy: "none",
    outlier_method: "none",
    outlier_threshold: 1.5,
    normalization: "none",
    encoding: "none",
    ordinal_map:
      fieldType === "categorical"
        ? Object.fromEntries(categories.map((value, index) => [value, Number(((index + 1) / categories.length).toFixed(2))]))
        : undefined,
    binary_map: fieldType === "binary" ? { true: 1, false: 0, "1": 1, "0": 0 } : undefined,
  };
}

function numericSamples(column?: PreviewColumn) {
  if (!column) return [];
  return column.sample_values
    .map((value) => Number(String(value).replace(",", ".")))
    .filter((value) => Number.isFinite(value));
}

function recommendationFor(field: FieldConfig, column?: PreviewColumn, fieldProfile?: FieldProfile) {
  if (fieldProfile?.recommendations.length) {
    return fieldProfile.recommendations.map((recommendation) => recommendation.message);
  }
  const notes: string[] = [];
  const samples = numericSamples(column);
  if (field.field_type === "numeric") {
    notes.push("Для расчета обычно нужна нормализация: minmax для рейтинга, robust при сильных выбросах.");
    if ((column?.missing_count ?? 0) > 0) {
      notes.push("Есть пропуски. Для числового поля безопасный вариант: median.");
    }
    if (samples.length >= 3) {
      const min = Math.min(...samples);
      const max = Math.max(...samples);
      const avg = samples.reduce((sum, item) => sum + item, 0) / samples.length;
      if (max > avg * 3 || min < avg / 3) {
        notes.push("В примерах заметен возможный выброс. Проверьте iqr_clip или robust normalization.");
      }
    }
  }
  if (field.field_type === "categorical") {
    notes.push("Если категории имеют порядок предпочтения, используйте ordinal encoding и задайте шкалу.");
    if ((column?.unique_count ?? 0) > 12) {
      notes.push("Много уникальных значений. Возможно, это справочник или текст, а не критерий.");
    }
  }
  if (field.field_type === "binary") {
    notes.push("Для бинарного признака обычно подходит binary_map: true = 1, false = 0.");
  }
  if (field.field_type === "text") {
    notes.push("Текстовые поля чаще используют как описание, а не как критерий расчета.");
  }
  return notes;
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

function criteriaDefaults(fields: FieldConfig[]): CriterionConfig[] {
  const analyticFields = fields.filter((field) => field.field_type !== "text" && field.include_in_output);
  const weight = analyticFields.length ? Number((1 / analyticFields.length).toFixed(4)) : 1;
  return analyticFields.map((field) => ({
    key: field.key,
    name: field.key.replace(/_/g, " "),
    weight,
    type: field.field_type === "categorical" ? "categorical" : field.field_type === "binary" ? "binary" : "numeric",
    direction: minimizeHints.some((hint) => field.key.includes(hint)) ? "minimize" : "maximize",
    scale_map: field.ordinal_map ?? field.binary_map,
  }));
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

function formatMetricValue(value: unknown) {
  if (value === null || value === undefined) return "-";
  if (Array.isArray(value)) return `${value.length} записей`;
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
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
  "Risky, preprocessing is recommended": "Есть риски, рекомендуется предобработка",
  "Not ready for reliable analysis": "Не готов к надежному анализу",
  "No fields are currently suitable for comparative analysis.": "Нет полей, подходящих для сравнительного анализа.",
  "Dataset has fewer than 3 analytic fields, comparison may be unstable.": "В датасете меньше 3 аналитических полей, сравнение может быть неустойчивым.",
  "Some non-numeric fields look like identifiers or free text and are weak criteria.": "Некоторые нечисловые поля похожи на идентификаторы или свободный текст и слабо подходят как критерии.",
  "Dataset is very small; analog search and statistics may be fragile.": "Датасет очень мал, поиск аналогов и статистика могут быть неустойчивыми.",
};

const FIELD_TYPE_LABELS: Record<string, string> = {
  numeric: "Числовой",
  categorical: "Категориальный",
  binary: "Бинарный",
  text: "Текстовый",
};

const METHOD_LABELS: Record<string, string> = {
  none: "Не применять",
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
  analysis_mode?: AnalysisMode;
  project_id?: number | null;
  scenario_title?: string | null;
};

type SavedWorkflowState = {
  activeStage?: StageId;
  datasetFileId?: number | null;
  preview?: PreviewResponse | null;
  profile?: FieldProfile[];
  quality?: DatasetQualityReport | null;
  recommendedWeights?: Record<string, number>;
  weightNotes?: string[];
  fields?: FieldConfig[];
  criteria?: CriterionConfig[];
  analysisMode?: AnalysisMode;
  targetRowId?: string;
  result?: PipelineResult | null;
  activeProjectId?: number | null;
  historyProjectFilter?: number | "all";
  scenarioTitle?: string;
  sourceFilename?: string;
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
    .map((group) => `<tr><td>${escapeHtml(translateAnalysisText(group.label))}</td><td>${group.object_ids.map((id) => `№${escapeHtml(id)}`).join(", ")}</td></tr>`)
    .join("");
  const dominanceRows = summaryList<{ dominant_object_id: string; dominated_object_id: string; criteria_count: number }>(
    result.analysis_summary,
    "dominance_pairs",
  )
    .map((item) => `<tr><td>№${escapeHtml(item.dominant_object_id)}</td><td>№${escapeHtml(item.dominated_object_id)}</td><td>${item.criteria_count}</td></tr>`)
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
  </style>
</head>
<body>
  <h1>Отчет сравнительного анализа</h1>
  <p>Сформировано: ${new Date().toLocaleString("ru-RU")}</p>
  <h2>Сводка</h2>
  ${summaryCards}
  <p class="note">${escapeHtml(translateAnalysisText(result.analysis_summary.ranking_stability_note ?? "Нет данных об устойчивости рейтинга."))}</p>
  ${confidenceNotes ? `<h3>Замечания к расчету</h3><ul>${confidenceNotes}</ul>` : ""}
  <h2>Критерии</h2>
  <table><thead><tr><th>Название</th><th>Поле датасета</th><th>Вес</th><th>Направление</th></tr></thead><tbody>${criteriaRows}</tbody></table>
  ${sensitivityRows ? `<h2>Чувствительность критериев</h2><table><thead><tr><th>Критерий</th><th>Вес</th><th>Разброс</th><th>Индекс</th><th>Пояснение</th></tr></thead><tbody>${sensitivityRows}</tbody></table>` : ""}
  ${analogRows ? `<h2>Группы аналогов</h2><table><thead><tr><th>Группа</th><th>Объекты</th></tr></thead><tbody>${analogRows}</tbody></table>` : ""}
  ${dominanceRows ? `<h2>Доминирование объектов</h2><table><thead><tr><th>Доминирующий объект</th><th>Уступающий объект</th><th>Критериев</th></tr></thead><tbody>${dominanceRows}</tbody></table>` : ""}
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

function contributionLabel(value: unknown) {
  const text = String(value ?? "-");
  return text.length > 18 ? `${text.slice(0, 18)}...` : text;
}

function ResultBarChart({
  result,
  mode,
  selectedId,
  onSelect,
}: {
  result: PipelineResult;
  mode: AnalysisMode;
  selectedId: string;
  onSelect: (objectId: string) => void;
}) {
  const rows = result.ranking.slice(0, 10);
  const maxValue = Math.max(...rows.map((item) => chartValue(item, mode)), 0.0001);
  return (
    <div className="analytics-card wide-chart">
      <div className="chart-head">
        <div>
          <span className="section-kicker">Bar chart</span>
          <h3>{mode === "analog_search" ? "Близость аналогов" : "Итоговые оценки"}</h3>
        </div>
        <small>Нажмите на столбец, чтобы разобрать объект</small>
      </div>
      <div className="result-bars">
        {rows.map((item) => {
          const value = chartValue(item, mode);
          const width = Math.max(3, (value / maxValue) * 100);
          return (
            <button
              className={`result-bar ${selectedId === item.object_id ? "active" : ""}`}
              key={item.object_id}
              onClick={() => onSelect(item.object_id)}
              title={`${item.title}: ${compactNumber(value)}`}
            >
              <span className="result-bar-rank">#{item.rank}</span>
              <span className="result-bar-label">{item.title}</span>
              <span className="result-bar-track">
                <span style={{ width: `${width}%` }} />
              </span>
              <strong>{compactNumber(value)}</strong>
            </button>
          );
        })}
      </div>
    </div>
  );
}

function RadarChart({ baseline, selected }: { baseline?: RankedItem; selected?: RankedItem }) {
  const source = selected ?? baseline;
  const contributions = source?.contributions.slice(0, 8) ?? [];
  const count = contributions.length;
  const center = 150;
  const radius = 104;
  const levels = [0.25, 0.5, 0.75, 1];
  const angleFor = (index: number) => (Math.PI * 2 * index) / Math.max(count, 1) - Math.PI / 2;
  const pointFor = (value: number, index: number) => {
    const angle = angleFor(index);
    const safeValue = Math.max(0, Math.min(1, value));
    return {
      x: center + Math.cos(angle) * radius * safeValue,
      y: center + Math.sin(angle) * radius * safeValue,
    };
  };
  const polygonFor = (item?: RankedItem) =>
    item?.contributions
      .slice(0, count)
      .map((contribution, index) => {
        const point = pointFor(contribution.normalized_value, index);
        return `${point.x},${point.y}`;
      })
      .join(" ") ?? "";

  return (
    <div className="analytics-card radar-card">
      <div className="chart-head">
        <div>
          <span className="section-kicker">Radar chart</span>
          <h3>Профиль критериев</h3>
        </div>
      </div>
      {count ? (
        <>
          <svg className="radar-svg" viewBox="0 0 300 300" role="img" aria-label="Профиль критериев">
            {levels.map((level) => (
              <polygon
                key={level}
                points={contributions.map((_, index) => {
                  const point = pointFor(level, index);
                  return `${point.x},${point.y}`;
                }).join(" ")}
                className="radar-grid"
              />
            ))}
            {contributions.map((contribution, index) => {
              const edge = pointFor(1, index);
              const label = pointFor(1.18, index);
              return (
                <g key={contribution.key}>
                  <line x1={center} y1={center} x2={edge.x} y2={edge.y} className="radar-axis" />
                  <text x={label.x} y={label.y} textAnchor="middle" dominantBaseline="middle">
                    {contributionLabel(contribution.name)}
                  </text>
                </g>
              );
            })}
            {baseline && <polygon points={polygonFor(baseline)} className="radar-shape baseline" />}
            {selected && <polygon points={polygonFor(selected)} className="radar-shape selected" />}
          </svg>
          <div className="chart-legend">
            <span><i className="legend-dot baseline" />Лидер</span>
            <span><i className="legend-dot selected" />Выбранный объект</span>
          </div>
        </>
      ) : (
        <p>Нет данных по критериям для построения профиля.</p>
      )}
    </div>
  );
}

function ContributionWaterfall({ item }: { item?: RankedItem }) {
  const contributions = item?.contributions ?? [];
  const total = contributions.reduce((sum, contribution) => sum + Math.max(0, contribution.contribution), 0);
  let cumulative = 0;
  return (
    <div className="analytics-card waterfall-card">
      <div className="chart-head">
        <div>
          <span className="section-kicker">Waterfall</span>
          <h3>Вклад критериев</h3>
        </div>
        {item ? <small>{item.title}</small> : null}
      </div>
      {item && contributions.length ? (
        <div className="waterfall-list">
          {contributions.map((contribution) => {
            const value = Math.max(0, contribution.contribution);
            const left = total > 0 ? (cumulative / total) * 100 : 0;
            const width = total > 0 ? Math.max(2, (value / total) * 100) : 0;
            cumulative += value;
            return (
              <div className="waterfall-row" key={contribution.key} title={`${contribution.name}: ${compactNumber(value)}`}>
                <span>{contribution.name}</span>
                <div className="waterfall-track">
                  <i style={{ left: `${left}%`, width: `${width}%` }} />
                </div>
                <strong>{compactNumber(value)}</strong>
              </div>
            );
          })}
          <div className="waterfall-total">
            <span>Итоговая сумма вкладов</span>
            <strong>{compactNumber(total)}</strong>
          </div>
        </div>
      ) : (
        <p>Выберите объект на диаграмме рейтинга, чтобы увидеть вклад критериев.</p>
      )}
    </div>
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
  const [preview, setPreview] = useState<PreviewResponse | null>(savedWorkflow.preview ?? null);
  const [profile, setProfile] = useState<FieldProfile[]>(savedWorkflow.profile ?? []);
  const [quality, setQuality] = useState<DatasetQualityReport | null>(savedWorkflow.quality ?? null);
  const [recommendedWeights, setRecommendedWeights] = useState<Record<string, number>>(savedWorkflow.recommendedWeights ?? {});
  const [weightNotes, setWeightNotes] = useState<string[]>(savedWorkflow.weightNotes ?? []);
  const [fields, setFields] = useState<FieldConfig[]>(savedWorkflow.fields ?? []);
  const [criteria, setCriteria] = useState<CriterionConfig[]>(savedWorkflow.criteria ?? []);
  const [analysisMode, setAnalysisMode] = useState<AnalysisMode>(savedWorkflow.analysisMode ?? "analog_search");
  const [targetRowId, setTargetRowId] = useState(savedWorkflow.targetRowId ?? "");
  const [result, setResult] = useState<PipelineResult | null>(savedWorkflow.result ?? null);
  const [selectedResultId, setSelectedResultId] = useState(savedWorkflow.result?.ranking?.[0]?.object_id ?? "");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const totalWeight = useMemo(() => criteria.reduce((sum, item) => sum + Number(item.weight || 0), 0), [criteria]);
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
  const reportHtml = useMemo(() => (result ? buildHtmlReport(result, criteria) : ""), [result, criteria]);
  const selectedResult = useMemo(
    () => result?.ranking.find((item) => item.object_id === selectedResultId) ?? result?.ranking[0],
    [result, selectedResultId],
  );

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
    setPreview(null);
    setProfile([]);
    setQuality(null);
    setRecommendedWeights({});
    setWeightNotes([]);
    setFields([]);
    setCriteria([]);
    setAnalysisMode("analog_search");
    setTargetRowId("");
    setResult(null);
    setSelectedResultId("");
    setActiveProjectId(null);
    setHistoryProjectFilter("all");
    setHistoryCompareIds([]);
    setScenarioTitle("Сценарий сравнительного анализа");
    setReportPreviewOpen(false);
    setError(null);
    localStorage.removeItem(WORKFLOW_STORAGE_KEY);
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
    if (result?.ranking.length && !result.ranking.some((item) => item.object_id === selectedResultId)) {
      setSelectedResultId(result.ranking[0].object_id);
    }
  }, [result, selectedResultId]);

  useEffect(() => {
    const state: SavedWorkflowState = {
      activeStage,
      datasetFileId,
      preview,
      profile,
      quality,
      recommendedWeights,
      weightNotes,
      fields,
      criteria,
      analysisMode,
      targetRowId,
      result,
      activeProjectId,
      historyProjectFilter,
      scenarioTitle,
      sourceFilename,
    };
    localStorage.setItem(WORKFLOW_STORAGE_KEY, JSON.stringify(state));
  }, [
    activeStage,
    datasetFileId,
    preview,
    profile,
    quality,
    recommendedWeights,
    weightNotes,
    fields,
    criteria,
    analysisMode,
    targetRowId,
    result,
    activeProjectId,
    historyProjectFilter,
    scenarioTitle,
    sourceFilename,
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
      setActiveProjectId(project.id);
    } catch (projectError) {
      setError(projectError instanceof Error ? projectError.message : "Не удалось создать проект");
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
      setFields(parameters.fields);
    }
    if (Array.isArray(parameters.criteria)) {
      setCriteria(parameters.criteria);
    }
    setTargetRowId(parameters.target_row_id ?? "");
    setAnalysisMode(parameters.analysis_mode === "rating" ? "rating" : "analog_search");
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
    try {
      const response = await profileFile(file);
      setDatasetFileId(response.dataset_file_id ?? null);
      setSourceFilename(file.name);
      const inferredFields = response.profile.fields.map((field) => ({
        ...field.recommended_config,
        missing_strategy: "none",
        outlier_method: "none",
        normalization: "none",
        encoding: "none",
      }));
      setPreview(response.preview);
      setProfile(response.profile.fields);
      setQuality(response.profile.quality);
      setRecommendedWeights(response.profile.recommended_weights);
      setWeightNotes(response.profile.weight_notes);
      setFields(inferredFields);
      setCriteria(criteriaDefaults(inferredFields));
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
      const response = await runPipeline(
        file,
        fields,
        criteria,
        analysisMode === "analog_search" ? targetRowId : undefined,
        analysisMode,
        token || undefined,
        activeProjectId,
        scenarioTitle,
        undefined,
        datasetFileId,
        sourceFilename || file?.name,
      );
      setResult(response);
      setSelectedResultId(response.ranking[0]?.object_id ?? "");
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

  function applyRecommendedFieldSettings(index: number) {
    const field = fields[index];
    if (!field) return;
    const fieldProfile = profile.find((item) => item.key === field.key);
    if (fieldProfile) {
      updateField(index, fieldProfile.recommended_config);
      return;
    }
    if (field.field_type === "numeric") {
      updateField(index, {
        missing_strategy: "median",
        outlier_method: "iqr_clip",
        normalization: "minmax",
        encoding: "none",
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
        encoding: "binary_map",
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

  function applyRecommendedWeights() {
    setCriteria((current) => {
      const patched = current.map((item) => ({
        ...item,
        weight: recommendedWeights[item.key] ?? item.weight,
      }));
      return patched;
    });
  }

  function rebuildCriteria() {
    setCriteria(criteriaDefaults(fields));
    setActiveStage("criteria");
  }

  function exportHtmlReport() {
    if (!result) return;
    downloadBlob("comparison-report.html", reportHtml, "text/html;charset=utf-8");
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

  function exportJsonReport() {
    if (!result) return;
    downloadBlob(
      "comparison-report.json",
      JSON.stringify({ generated_at: new Date().toISOString(), fields, criteria, result }, null, 2),
      "application/json;charset=utf-8",
    );
  }

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div className="brand">
          <div className="brand-mark">CA</div>
          <div>
            <strong>CompareLab</strong>
            <span>аналитическая система</span>
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

        <div className="sidebar-card">
          <span>Сумма весов</span>
          <strong>{totalWeight.toFixed(3)}</strong>
          <small>{Math.abs(totalWeight - 1) < 0.001 ? "Готово к расчету" : "Будет нормализовано автоматически"}</small>
        </div>
      </aside>

      <main className="workspace">
        <header className="topbar">
          <div>
            <p className="eyebrow">Рабочий процесс аналитического сравнения</p>
            <h1>Сравнительный анализ объектов</h1>
            <p className="subtitle">
              Полный рабочий процесс: загрузка данных, подготовка признаков, настройка весов и выпуск отчета.
            </p>
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
                    <span>{user.role === "admin" ? "администратор" : "пользователь"}</span>
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
            <div className="panel hero-panel">
              <div className="hero-copy">
                <span className="section-kicker">Этап 1</span>
                <h2>Загрузка исходного датасета</h2>
                <p>
                  Поддерживаются CSV, XLSX и JSON. После загрузки система нормализует имена колонок и построит паспорт
                  признаков.
                </p>
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
                  {loading ? "Анализируем файл..." : "Построить предпросмотр"}
                </button>
              </div>
            </div>

            <div className="panel">
              <div className="panel-head">
                <div>
                  <span className="section-kicker">Предпросмотр</span>
                  <h2>Первые строки датасета</h2>
                </div>
                {preview ? <span className="pill">{preview.rows_total} строк</span> : null}
              </div>
              {preview ? (
                <>
                  {preview.warnings.length > 0 ? (
                    <div className="alert warning">
                      {preview.warnings.slice(0, 4).map((warning) => (
                        <p key={warning}>{warning}</p>
                      ))}
                    </div>
                  ) : null}
                  <div className="table-wrap dataset-table wide">
                    <table>
                      <thead>
                        <tr>
                          {preview.columns.map((column) => (
                            <th key={column.normalized_name}>{column.normalized_name}</th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {preview.preview_rows.map((row, rowIndex) => (
                          <tr key={`preview-row-${rowIndex}`}>
                            {preview.columns.map((column) => (
                              <td key={`${rowIndex}-${column.normalized_name}`}>
                                {row[column.normalized_name] === null || row[column.normalized_name] === ""
                                  ? <span className="muted-cell">пусто</span>
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
                <div className="empty-state">Здесь появится структура датасета после загрузки файла.</div>
              )}
            </div>

            {preview ? (
              <div className="panel schema-panel">
                <div className="panel-head">
                  <div>
                    <span className="section-kicker">Паспорт данных</span>
                    <h2>Колонки и типы признаков</h2>
                  </div>
                  <button onClick={() => setActiveStage("preprocessing")}>Перейти к подготовке</button>
                </div>
                <div className="table-wrap compact">
                  <table>
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
                      {preview.columns.map((column) => (
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
                <p>Система не применяет преобразования автоматически: выберите только то, что действительно нужно.</p>
              </div>
              <button onClick={rebuildCriteria} disabled={fields.length === 0}>Сформировать критерии</button>
            </div>

            {fields.length > 0 ? (
              <>
                {quality ? (
                  <div className={`quality-card ${quality.level}`}>
                    <div className="quality-score">
                      <span>Качество данных</span>
                      <strong>{quality.score.toFixed(0)}</strong>
                      <small>{translateQualityText(quality.readiness_label)}</small>
                    </div>
                    <div className="quality-body">
                      <div className="quality-metrics">
                        <div><span>Аналитических полей</span><strong>{quality.analytic_fields_count}</strong></div>
                        <div><span>Пропусков</span><strong>{quality.total_missing_values}</strong></div>
                        <div><span>Выбросов IQR</span><strong>{quality.total_outliers_iqr}</strong></div>
                        <div><span>Текстовых полей</span><strong>{quality.text_fields_count}</strong></div>
                      </div>
                      {quality.issues.length > 0 ? (
                        <div className="quality-issues">
                          {quality.issues.slice(0, 4).map((issue) => (
                            <p key={issue.code}>
                              <b>{translateStatus(issue.severity)}</b> · {translateQualityText(issue.message)}
                              {issue.affected_fields.length ? ` Поля: ${issue.affected_fields.slice(0, 4).join(", ")}` : ""}
                            </p>
                          ))}
                        </div>
                      ) : (
                        <p className="quality-ok">Серьезных проблем с качеством данных не найдено.</p>
                      )}
                    </div>
                  </div>
                ) : null}
                <div className="prep-summary">
                  <div className="metric">
                    <span>Полей</span>
                    <strong>{fields.length}</strong>
                  </div>
                  <div className="metric">
                    <span>С пропусками</span>
                    <strong>{preview?.columns.filter((column) => column.missing_count > 0).length ?? 0}</strong>
                  </div>
                  <div className="metric">
                    <span>Числовых кандидатов</span>
                    <strong>{fields.filter((field) => field.field_type === "numeric").length}</strong>
                  </div>
                  <div className="metric">
                    <span>Активных преобразований</span>
                    <strong>{fields.filter((field) => field.normalization !== "none" || field.encoding !== "none" || field.outlier_method !== "none" || field.missing_strategy !== "none").length}</strong>
                  </div>
                </div>
                <div className="field-board">
                  {fields.map((field, index) => (
                    <article className="field-card" key={field.key}>
                    <div className="field-card-head">
                      <div>
                        <strong>{field.key}</strong>
                        <span>{fieldTypeLabel(field.field_type)}</span>
                      </div>
                      <label className="switch">
                        <input
                          type="checkbox"
                          checked={field.include_in_output}
                          onChange={(event) => updateField(index, { include_in_output: event.target.checked })}
                        />
                        <span />
                      </label>
                    </div>
                    <div className="recommendation-box">
                      <div className="recommendation-head">
                        <span className="tag">Рекомендации</span>
                        <button className="link-button" onClick={() => applyRecommendedFieldSettings(index)}>
                          применить
                        </button>
                      </div>
                      {recommendationFor(
                        field,
                        preview?.columns.find((column) => column.normalized_name === field.key),
                        profile.find((item) => item.key === field.key),
                      ).map((note) => (
                        <p key={note}>{note}</p>
                      ))}
                    </div>
                    {field.field_type === "numeric" ? (
                      <MiniDistribution
                        column={preview?.columns.find((column) => column.normalized_name === field.key)}
                        fieldProfile={profile.find((item) => item.key === field.key)}
                      />
                    ) : profile.find((item) => item.key === field.key)?.top_categories.length ? (
                      <div className="category-values">
                        {profile.find((item) => item.key === field.key)?.top_categories.slice(0, 8).map((point) => (
                          <span key={point.label}>{point.label} · {point.value}</span>
                        ))}
                      </div>
                    ) : null}
                    <div className="control-grid">
                      <label>
                        Тип поля
                        <select
                          value={field.field_type}
                          onChange={(event) => updateField(index, { field_type: event.target.value as FieldConfig["field_type"] })}
                        >
                          <option value="numeric">{fieldTypeLabel("numeric")}</option>
                          <option value="categorical">{fieldTypeLabel("categorical")}</option>
                          <option value="binary">{fieldTypeLabel("binary")}</option>
                          <option value="text">{fieldTypeLabel("text")}</option>
                        </select>
                      </label>
                      <label>
                        Пропуски
                        <select value={field.missing_strategy} onChange={(event) => updateField(index, { missing_strategy: event.target.value })}>
                          <option value="none">{methodLabel("none")}</option>
                          <option value="median">{methodLabel("median")}</option>
                          <option value="mean">{methodLabel("mean")}</option>
                          <option value="mode">{methodLabel("mode")}</option>
                          <option value="drop_row">{methodLabel("drop_row")}</option>
                          <option value="constant">{methodLabel("constant")}</option>
                        </select>
                      </label>
                      <label>
                        Выбросы
                        <select value={field.outlier_method} onChange={(event) => updateField(index, { outlier_method: event.target.value })}>
                          <option value="none">{methodLabel("none")}</option>
                          <option value="iqr_clip">{methodLabel("iqr_clip")}</option>
                          <option value="iqr_remove">{methodLabel("iqr_remove")}</option>
                          <option value="zscore_clip">{methodLabel("zscore_clip")}</option>
                          <option value="zscore_remove">{methodLabel("zscore_remove")}</option>
                        </select>
                      </label>
                      <label>
                        Нормализация
                        <select value={field.normalization} onChange={(event) => updateField(index, { normalization: event.target.value })}>
                          <option value="none">{methodLabel("none")}</option>
                          <option value="minmax">{methodLabel("minmax")}</option>
                          <option value="zscore">{methodLabel("zscore")}</option>
                          <option value="robust">{methodLabel("robust")}</option>
                          <option value="log_minmax">{methodLabel("log_minmax")}</option>
                        </select>
                      </label>
                    </div>
                    </article>
                  ))}
                </div>
              </>
            ) : (
              <div className="empty-state">Сначала загрузите файл на этапе «Данные».</div>
            )}
          </section>
        ) : null}

        {activeStage === "criteria" ? (
          <section className="panel">
            <div className="panel-head">
              <div>
                <span className="section-kicker">Этап 3</span>
                <h2>Целевой объект и критерии</h2>
                <p>Выберите объект, для которого нужно найти аналоги, затем задайте критерии близости и их веса.</p>
              </div>
              <div className="criteria-actions">
                <div className="weight-meter">
                  <span>Σ весов</span>
                  <strong>{totalWeight.toFixed(3)}</strong>
                </div>
                <button className="ghost-button" onClick={applyRecommendedWeights} disabled={!Object.keys(recommendedWeights).length}>
                  Рекомендовать веса
                </button>
                <button onClick={normalizeWeights} disabled={criteria.length === 0 || totalWeight <= 0}>
                  Нормализовать веса
                </button>
              </div>
            </div>

            {criteria.length > 0 ? (
              <>
                <div className="project-panel">
                  <label>
                    Проект
                    <select
                      value={activeProjectId ?? ""}
                      onChange={(event) => setActiveProjectId(event.target.value ? Number(event.target.value) : null)}
                      disabled={!user}
                    >
                      <option value="">Без проекта</option>
                      {projects.map((project) => (
                        <option key={project.id} value={project.id}>
                          {project.name}
                        </option>
                      ))}
                    </select>
                  </label>
                  <label>
                    Название сценария
                    <input value={scenarioTitle} onChange={(event) => setScenarioTitle(event.target.value)} />
                  </label>
                  <label>
                    Новый проект
                    <input value={newProjectName} onChange={(event) => setNewProjectName(event.target.value)} disabled={!user} />
                  </label>
                  <button className="ghost-button" onClick={handleCreateProject} disabled={!user || loading}>
                    Создать проект
                  </button>
                </div>
                <div className="target-panel">
                  <label>
                    Режим анализа
                    <select value={analysisMode} onChange={(event) => setAnalysisMode(event.target.value as AnalysisMode)}>
                      <option value="analog_search">Поиск аналогов для целевого объекта</option>
                      <option value="rating">Общий рейтинг объектов</option>
                    </select>
                  </label>
                  <label>
                    Целевой объект
                    <select
                      value={targetRowId}
                      onChange={(event) => setTargetRowId(event.target.value)}
                      disabled={analysisMode !== "analog_search"}
                    >
                      {(preview?.normalized_dataset?.rows ?? []).map((row) => {
                        const label = String(row.values.name ?? row.values.title ?? row.values.object_name ?? `Объект ${row.id}`);
                        return <option key={row.id} value={row.id}>{row.id} · {label}</option>;
                      })}
                    </select>
                  </label>
                  <div className="target-explain">
                    {analysisMode === "analog_search"
                      ? "Система сравнит остальные объекты с выбранным целевым объектом по нормализованным критериям и отсортирует аналоги по близости."
                      : "Система построит общий рейтинг по направлению критериев: больше/меньше/целевое значение."}
                  </div>
                </div>
                {weightNotes.length > 0 ? (
                  <div className="weight-notes">
                    {weightNotes.map((note) => (
                      <p key={note}>{note}</p>
                    ))}
                  </div>
                ) : null}
                <div className="criteria-board">
                  {criteria.map((criterion, index) => (
                    <article className="criterion-card" key={criterion.key}>
                    <div className="criterion-main">
                      <span className="tag">{criterion.type}</span>
                      <h3>{criterion.name}</h3>
                      <p>{criterion.key}</p>
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
                      Направление
                      <select
                        value={criterion.direction}
                        onChange={(event) => updateCriterion(index, { direction: event.target.value as CriterionConfig["direction"] })}
                      >
                        <option value="maximize">Больше лучше</option>
                        <option value="minimize">Меньше лучше</option>
                        <option value="target">Ближе к цели</option>
                      </select>
                    </label>
                      {recommendedWeights[criterion.key] !== undefined ? (
                        <div className="recommended-weight">
                          Рекомендация системы: <strong>{recommendedWeights[criterion.key].toFixed(4)}</strong>
                        </div>
                      ) : null}
                    </article>
                  ))}
                </div>
              </>
            ) : (
              <div className="empty-state">Сформируйте критерии после настройки подготовки данных.</div>
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
                      ? "Список объектов, наиболее близких к выбранному целевому объекту по заданным критериям."
                      : "Итоговый рейтинг, объяснение результата и экспорт отчета сравнения."}
                  </p>
                </div>
                <div className="report-actions">
                  <button className="ghost-button" onClick={() => setReportPreviewOpen(true)} disabled={!result}>Предпросмотр</button>
                  <button className="ghost-button" onClick={exportJsonReport} disabled={!result}>JSON</button>
                  <button className="ghost-button" onClick={exportHtmlReport} disabled={!result}>HTML</button>
                  <button onClick={exportDocxReport} disabled={!result || loading}>DOCX</button>
                </div>
              </div>

              {result ? (
                <>
                  <div className="metric-row">
                    {summaryCardEntries(result.analysis_summary).map(([key, value]) => (
                      <div className="metric" key={key}>
                        <span>{labelForSummaryKey(key)}</span>
                        <strong>{formatSummaryValue(key, value)}</strong>
                      </div>
                    ))}
                  </div>
                  <div className="analytics-grid">
                    <ResultBarChart
                      result={result}
                      mode={analysisMode}
                      selectedId={selectedResult?.object_id ?? ""}
                      onSelect={setSelectedResultId}
                    />
                    <RadarChart baseline={result.ranking[0]} selected={selectedResult} />
                    <ContributionWaterfall item={selectedResult} />
                  </div>
                  <div className="insight-grid">
                    <div className="insight-card">
                      <span className="section-kicker">Объяснимость</span>
                      <h3>Чувствительность критериев</h3>
                      {summaryList<{ key: string; name: string; sensitivity_index: number; note: string }>(result.analysis_summary, "sensitivity")
                        .slice(0, 4)
                        .map((item) => (
                          <div className="insight-row" key={item.key}>
                            <span>{item.name}</span>
                            <strong>{Number(item.sensitivity_index).toFixed(4)}</strong>
                            <small>{translateAnalysisText(item.note)}</small>
                          </div>
                        ))}
                    </div>
                    <div className="insight-card">
                      <span className="section-kicker">Надежность</span>
                      <h3>Доверие и устойчивость</h3>
                      <p>{translateAnalysisText(result.analysis_summary.ranking_stability_note ?? "Нет данных об устойчивости рейтинга.")}</p>
                      {summaryList<string>(result.analysis_summary, "confidence_notes").map((item, index) => (
                        <p key={`${item}-${index}`}>{translateAnalysisText(item)}</p>
                      ))}
                    </div>
                    <div className="insight-card">
                      <span className="section-kicker">Аналоги</span>
                      <h3>Группы аналогов</h3>
                      {summaryList<{ label: string; object_ids: string[] }>(result.analysis_summary, "analog_groups").length ? (
                        summaryList<{ label: string; object_ids: string[] }>(result.analysis_summary, "analog_groups").map((group) => (
                          <div className="group-chip" key={group.label}>
                            <strong>{translateAnalysisText(group.label)}</strong>
                            <span>{group.object_ids.length} объектов</span>
                          </div>
                        ))
                      ) : (
                        <p>Группы аналогов доступны в режиме поиска аналогов.</p>
                      )}
                    </div>
                  </div>
                  <div className="ranking-table">
                    {result.ranking.map((item) => (
                      <article className="result-card" key={item.object_id}>
                        <div className="result-rank">#{item.rank}</div>
                        <div className="result-body">
                          <div className="result-title">
                            <h3>{item.title}</h3>
                            <strong>
                              {analysisMode === "analog_search" && item.similarity_to_target !== null && item.similarity_to_target !== undefined
                                ? item.similarity_to_target.toFixed(4)
                                : item.score.toFixed(4)}
                            </strong>
                          </div>
                          <p>{translateAnalysisText(item.explanation)}</p>
                          <div className="bar-list">
                            {item.contributions.map((contribution) => (
                              <div className="bar-item" key={contribution.key}>
                                <span>{contribution.name}</span>
                                <div className="bar-track">
                                  <div className="bar-fill" style={{ width: `${Math.max(4, contribution.contribution * 100)}%` }} />
                                </div>
                                <strong>{contribution.contribution.toFixed(3)}</strong>
                              </div>
                            ))}
                          </div>
                        </div>
                      </article>
                    ))}
                  </div>
                </>
              ) : (
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
                            <span>Confidence</span>
                            <strong>{summaryNumber(summary, "confidence_score").toFixed(4)}</strong>
                          </div>
                        </div>
                      );
                    })}
                    <div className="compare-delta">
                      <span>Δ лучшей оценки</span>
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
                          <strong>{item.dataset_file_id && item.result_file_id ? "файловое хранилище" : "только БД"}</strong>
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
                          <td>{item.is_active ? "активен" : "заблокирован"}</td>
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
                <button className="ghost-button" onClick={exportJsonReport}>Скачать JSON</button>
                <button className="ghost-button" onClick={exportHtmlReport}>Скачать HTML</button>
                <button onClick={exportDocxReport} disabled={loading}>Скачать DOCX</button>
              </div>
              <iframe className="report-preview-frame" title="Предпросмотр отчета" srcDoc={reportHtml} />
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
