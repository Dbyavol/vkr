# Coverage Report

Дата прогона: 2026-04-29

Команда:

```powershell
.\.venv\Scripts\python.exe -m pytest --cov=backend/app --cov-report=term-missing --cov-report=xml:coverage.xml --cov-report=html:htmlcov -q
```

Итог:

- Всего тестов: `17`
- Статус: `17 passed`
- Общее покрытие backend: `80%`
- XML-отчет: [coverage.xml](/C:/Users/Stepan/Documents/New%20project/coverage.xml)
- HTML-отчет: [htmlcov/index.html](/C:/Users/Stepan/Documents/New%20project/htmlcov/index.html)

Наиболее полно покрытые части:

- `backend/app/schemas/*`
- `backend/app/services/user_service.py`
- `backend/app/db/*`
- `backend/app/models/*`

Зоны для дальнейшего усиления тестирования:

- `backend/app/services/analysis_engine.py` — `61%`
- `backend/app/services/file_service.py` — `61%`
- `backend/app/services/preprocessing_engine.py` — `61%`
- `backend/app/services/object_service.py` — `60%`
- `backend/app/api/files.py` — `63%`
- `backend/app/api/objects.py` — `60%`

Интерпретация результата:

Текущее покрытие подтверждает работоспособность основных пользовательских сценариев и критической бизнес-логики нового модульного монолита. При этом наибольший потенциал роста находится в дополнительных ветках обработки ошибок, редких сценариях API и расширенных аналитических ветвлениях.
