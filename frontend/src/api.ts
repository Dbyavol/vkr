import type {
  AdminStats,
  AnalysisMode,
  AuthResponse,
  AuthUser,
  ComparisonHistoryItem,
  CriterionConfig,
  PipelineProfileResponse,
  FieldConfig,
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

async function readError(response: Response, fallback: string) {
  try {
    const payload = await response.json();
    return payload?.detail?.message ?? payload?.detail ?? fallback;
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

  const response = await fetch(`${ORCHESTRATOR_URL}/pipeline/profile`, {
    method: "POST",
    body: form,
  });

  if (!response.ok) {
    throw new Error(await readError(response, "Не удалось построить профиль датасета"));
  }

  return response.json();
}

export async function runPipeline(
  file: File,
  fields: FieldConfig[],
  criteria: CriterionConfig[],
  targetRowId?: string,
  analysisMode: AnalysisMode = "rating",
  token?: string,
  projectId?: number | null,
  scenarioTitle?: string,
  parentHistoryId?: number | null,
): Promise<PipelineResult> {
  const form = new FormData();
  form.append("file", file);
  form.append(
    "config_json",
    JSON.stringify({
      fields,
      criteria,
      target_row_id: targetRowId || null,
      analysis_mode: analysisMode,
      top_n: 10,
      project_id: projectId ?? null,
      scenario_title: scenarioTitle || null,
      parent_history_id: parentHistoryId ?? null,
    }),
  );

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
