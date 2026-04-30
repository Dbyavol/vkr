# Load Testing (Smoke)

Ниже легкий сценарий нагрузочного smoke-теста без внешних зависимостей.

## Что проверяем

- Среднее время отклика (avg)
- Хвостовые задержки (p95/p99)
- Долю ошибок
- Распределение HTTP-статусов

## Запуск

```powershell
# Из корня репозитория
.\.venv\Scripts\python.exe .\scripts\load_smoke_test.py --base-url http://127.0.0.1:8000 --requests 200 --concurrency 12
```

Можно добавить целевые пути отдельно:

```powershell
.\.venv\Scripts\python.exe .\scripts\load_smoke_test.py --base-url http://127.0.0.1:8000 --path /health --path /api/v1/pipeline/profile-stored --requests 100 --concurrency 6
```

## Интерпретация

- Для `/health` p95 обычно должен быть низким и стабильным.
- Для тяжелых endpoint'ов (`pipeline/*`) p95/p99 будут выше, главное контролировать рост ошибок.
- Если `error_rate_pct > 1%`, стоит проверять логи и лимиты ресурсов (CPU/RAM/IO).
