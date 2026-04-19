# Import Service

Изолированный микросервис импорта файлов и первичной подготовки датасета.

## Назначение

Сервис отвечает за:

- прием CSV, XLSX и JSON
- построение preview загруженного файла
- нормализацию названий колонок
- первичное определение типов признаков
- преобразование файла в унифицированный датасет формата `rows[].values`
- формирование payload для передачи в `preprocessing-service`

## Роль в общей схеме

Рекомендуемый поток:

1. Пользователь загружает файл через `import-service`.
2. `import-service` строит preview и унифицирует структуру.
3. Файл и метаданные могут быть сохранены через `storage-service`.
4. Унифицированный датасет передается в `preprocessing-service`.
5. После предобработки датасет уходит в `comparative-analysis-service`.

## Основные endpoint'ы

- `POST /api/v1/imports/preview` — принять файл и вернуть preview
- `POST /api/v1/imports/parse-base64` — тот же разбор, но для base64-контента
- `POST /api/v1/imports/commit` — сформировать handoff payload для следующего шага pipeline

## Локальный запуск

```bash
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8060
```

## Пример preview

```bash
curl -X POST http://localhost:8060/api/v1/imports/preview ^
  -F "file=@example.csv"
```

## Пример commit

```bash
curl -X POST http://localhost:8060/api/v1/imports/commit ^
  -H "Content-Type: application/json" ^
  --data-binary "@example-commit.json"
```

## Что сервис возвращает

- количество строк
- описание колонок
- preview первых строк
- список предупреждений по нормализации названий
- унифицированный датасет для последующей предобработки

## Что можно расширить следующим шагом

- явное сохранение импорта в БД
- логирование ошибок по строкам и полям
- маппинг колонок пользователем
- интеграцию со `storage-service` по HTTP
- автоформирование рекомендуемой конфигурации для `preprocessing-service`
