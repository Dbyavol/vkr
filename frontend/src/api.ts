import type {
  AdminStats,
  AnalysisFilters,
  AnalysisMode,
  AuthResponse,
  AuthUser,
  ComparisonHistoryItem,
  CriterionConfig,
  PipelineProfileResponse,
  FieldConfig,
  PreprocessingRefreshResponse,
  PipelineResult,
  PreviewResponse,
  ProjectItem,
  SystemDashboard,
} from "./types";

const ORCHESTRATOR_URL =
  ((import.meta as unknown as { env?: { VITE_ORCHESTRATOR_URL?: string } }).env?.VITE_ORCHESTRATOR_URL ??
    "http://localhost:8050/api/v1").replace(/\/$/, "");
const AUTH_URL =
  ((import.meta as unknown as { env?: { VITE_AUTH_URL?: string } }).env?.VITE_AUTH_URL ??
    "http://localhost:8040/api/v1").replace(/\/$/, "");
const STORAGE_URL =
  ((import.meta as unknown as { env?: { VITE_STORAGE_URL?: string } }).env?.VITE_STORAGE_URL ??
    "http://localhost:8070/api/v1").replace(/\/$/, "");

type ApiValidationError = {
  loc?: unknown[];
  msg?: string;
  type?: string;
};

function mapKnownError(message: string, fallback: string) {
  const normalized = message.toLowerCase();
  if (normalized.includes("user already exists")) {
    return "Пользователь с таким email уже зарегистрирован.";
  }
  if (normalized.includes("invalid email or password")) {
    return "Неверный email или пароль.";
  }
  if (normalized.includes("missing bearer token") || normalized.includes("not authenticated")) {
    return "Необходимо войти в систему.";
  }
  if (normalized.includes("admin role required")) {
    return "Для действия нужны права администратора.";
  }
  return message || fallback;
}

function formatValidationErrors(errors: ApiValidationError[], fallback: string) {
  const messages = errors.map((error) => {
    const field = Array.isArray(error.loc) && error.loc.length > 0 ? String(error.loc[error.loc.length - 1]) : "";
    if (field === "email") {
      return "Введите корректный email.";
    }
    if (field === "password") {
      return "Пароль должен содержать минимум 6 символов.";
    }
    if (field === "full_name") {
      return "Имя должно содержать минимум 2 символа.";
    }
    return error.msg ? mapKnownError(error.msg, fallback) : fallback;
  });
  return Array.from(new Set(messages)).join(" ");
}

async function readError(response: Response, fallback: string) {
  try {
    const payload = await response.json();
    const detail = payload?.detail;
    if (Array.isArray(detail)) {
      return formatValidationErrors(detail, fallback);
    }
    const rawMessage = payload?.error?.message ?? detail?.message ?? payload?.message ?? detail;
    return typeof rawMessage === "string" ? mapKnownError(rawMessage, fallback) : fallback;
  } catch {
    return fallback;
  }
}

export async function previewFile(file: File): Promise<PreviewResponse> {
  const form = new FormData();
  form.append("file", file);

  const response = await fetch(`${ORCHESTRATOR_URL}/pipeline/preview`, {
    method: "POST",
    body: form,
  });

  if (!response.ok) {
    throw new Error(await readError(response, "Не удалось получить предпросмотр файла"));
  }

  return response.json();
}

export async function profileFile(file: File): Promise<PipelineProfileResponse> {
  const form = new FormData();
  form.append("file", file);
  form.append("detail_level", "preview");

  const response = await fetch(`${ORCHESTRATOR_URL}/pipeline/upload-profile`, {
    method: "POST",
    body: form,
  });

  if (!response.ok) {
    throw new Error(await readError(response, "Не удалось построить профиль датасета"));
  }

  return response.json();
}

export async function fetchStoredProfile(
  datasetFileId: number,
  filename?: string,
  options?: { histogramBins?: number; histogramBinsByField?: Record<string, number>; detailLevel?: "summary" | "detailed" },
): Promise<PipelineProfileResponse> {
  const response = await fetch(`${ORCHESTRATOR_URL}/pipeline/profile-stored`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      dataset_file_id: datasetFileId,
      filename: filename || null,
      histogram_bins: options?.histogramBins ?? 8,
      histogram_bins_by_field: options?.histogramBinsByField ?? {},
      profile_detail_level: options?.detailLevel ?? "detailed",
    }),
  });

  if (!response.ok) {
    throw new Error(await readError(response, "Не удалось загрузить расширенный профиль датасета"));
  }

  return response.json();
}

export async function runPipeline(
  file: File | null,
  fields: FieldConfig[],
  criteria: CriterionConfig[],
  targetRowId?: string,
  geoRadiusKm?: number | null,
  analysisMode: AnalysisMode = "rating",
  token?: string,
  projectId?: number | null,
  scenarioTitle?: string,
  parentHistoryId?: number | null,
  datasetFileId?: number | null,
  filename?: string,
  filterCriteria?: AnalysisFilters,
  includeStabilityScenarios = false,
  stabilityVariationPct = 10,
  topN = 10,
): Promise<PipelineResult> {
  const config = {
    fields,
    criteria,
    target_row_id: targetRowId || null,
    geo_radius_km: geoRadiusKm ?? null,
    analysis_mode: analysisMode,
    top_n: Math.max(1, Math.floor(topN)),
    filter_criteria: filterCriteria ?? null,
    include_stability_scenarios: includeStabilityScenarios,
    stability_variation_pct: stabilityVariationPct,
    project_id: projectId ?? null,
    scenario_title: scenarioTitle || null,
    parent_history_id: parentHistoryId ?? null,
  };

  if (datasetFileId) {
    const response = await fetch(`${ORCHESTRATOR_URL}/pipeline/run-stored`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        ...(token ? { Authorization: `Bearer ${token}` } : {}),
      },
      body: JSON.stringify({
        filename: filename || file?.name || "dataset",
        dataset_file_id: datasetFileId,
        config,
      }),
    });

    if (!response.ok) {
      throw new Error(await readError(response, "Не удалось выполнить расчет"));
    }

    return response.json();
  }

  if (!file) {
    throw new Error("Сначала загрузите датасет или восстановите сохраненный файл.");
  }

  const form = new FormData();
  form.append("file", file);
  form.append("config_json", JSON.stringify(config));

  const response = await fetch(`${ORCHESTRATOR_URL}/pipeline/run`, {
    method: "POST",
    body: form,
    headers: token ? { Authorization: `Bearer ${token}` } : undefined,
  });

  if (!response.ok) {
    throw new Error(await readError(response, "Не удалось выполнить расчет"));
  }

  return response.json();
}

export async function refreshPreprocessing(
  datasetFileId: number,
  fields: FieldConfig[],
  filename?: string,
  options?: { histogramBins?: number; histogramBinsByField?: Record<string, number>; detailLevel?: "summary" | "detailed" },
): Promise<PreprocessingRefreshResponse> {
  const response = await fetch(`${ORCHESTRATOR_URL}/pipeline/preprocess-refresh`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      dataset_file_id: datasetFileId,
      filename: filename || null,
      fields,
      histogram_bins: options?.histogramBins ?? 8,
      histogram_bins_by_field: options?.histogramBinsByField ?? {},
      profile_detail_level: options?.detailLevel ?? "detailed",
    }),
  });

  if (!response.ok) {
    throw new Error(await readError(response, "Не удалось применить настройки предобработки"));
  }

  return response.json();
}

export async function login(email: string, password: string): Promise<AuthResponse> {
  const response = await fetch(`${AUTH_URL}/auth/login`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ email, password }),
  });
  if (!response.ok) {
    throw new Error(await readError(response, "Не удалось войти"));
  }
  return response.json();
}

export async function register(fullName: string, email: string, password: string): Promise<AuthResponse> {
  const response = await fetch(`${AUTH_URL}/auth/register`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ full_name: fullName, email, password }),
  });
  if (!response.ok) {
    throw new Error(await readError(response, "Не удалось зарегистрироваться"));
  }
  return response.json();
}

export async function fetchMe(token: string): Promise<AuthUser> {
  const response = await fetch(`${AUTH_URL}/users/me`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  if (!response.ok) {
    throw new Error(await readError(response, "Сессия недействительна"));
  }
  return response.json();
}

export async function fetchAdminStats(token: string): Promise<AdminStats> {
  const response = await fetch(`${AUTH_URL}/admin/stats`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  if (!response.ok) {
    throw new Error(await readError(response, "Нет доступа к статистике"));
  }
  return response.json();
}

export async function fetchAdminUsers(token: string): Promise<AuthUser[]> {
  const response = await fetch(`${AUTH_URL}/admin/users`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  if (!response.ok) {
    throw new Error(await readError(response, "Нет доступа к пользователям"));
  }
  return response.json();
}

export async function fetchHistory(userId: number): Promise<ComparisonHistoryItem[]> {
  const response = await fetch(`${STORAGE_URL}/comparison-history?user_id=${userId}`);
  if (!response.ok) {
    throw new Error(await readError(response, "Не удалось получить историю"));
  }
  return response.json();
}

export async function fetchProjects(userId: number): Promise<ProjectItem[]> {
  const response = await fetch(`${STORAGE_URL}/projects?user_id=${userId}`);
  if (!response.ok) {
    throw new Error(await readError(response, "Не удалось получить проекты"));
  }
  return response.json();
}

export async function createProject(user: AuthUser, name: string, description = ""): Promise<ProjectItem> {
  const response = await fetch(`${STORAGE_URL}/projects`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      owner_user_id: user.id,
      owner_email: user.email,
      name,
      description,
    }),
  });
  if (!response.ok) {
    throw new Error(await readError(response, "Не удалось создать проект"));
  }
  return response.json();
}

export async function fetchSystemDashboard(token?: string): Promise<SystemDashboard> {
  const response = await fetch(`${ORCHESTRATOR_URL}/system/dashboard`, {
    headers: token ? { Authorization: `Bearer ${token}` } : undefined,
  });
  if (!response.ok) {
    throw new Error(await readError(response, "Не удалось получить dashboard системы"));
  }
  return response.json();
}

export async function downloadDocxReport(result: PipelineResult, criteria: CriterionConfig[]) {
  const response = await fetch(`${ORCHESTRATOR_URL}/reports/comparison.docx`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      title: "Отчет сравнительного анализа",
      criteria,
      result,
    }),
  });
  if (!response.ok) {
    throw new Error(await readError(response, "Не удалось сформировать DOCX-отчет"));
  }
  const blob = await response.blob();
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = "comparison-report.docx";
  link.click();
  URL.revokeObjectURL(url);
}

export async function uploadReportFile(fileName: string, blob: Blob): Promise<number> {
  const form = new FormData();
  form.append("file", blob, fileName);

  const response = await fetch(`${STORAGE_URL}/files/upload?purpose=comparison-report`, {
    method: "POST",
    body: form,
  });
  if (!response.ok) {
    throw new Error(await readError(response, "Не удалось сохранить отчет в хранилище"));
  }
  const data = await response.json();
  return Number(data.id);
}

export async function bindReportToHistory(historyId: number, resultFileId: number): Promise<void> {
  const response = await fetch(`${STORAGE_URL}/comparison-history/${historyId}/result-file`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ result_file_id: resultFileId }),
  });
  if (!response.ok) {
    throw new Error(await readError(response, "Не удалось привязать отчет к истории"));
  }
}
