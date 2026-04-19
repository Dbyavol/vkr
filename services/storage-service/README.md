# Storage Service

Изолированный микросервис хранения для универсальной информационно-аналитической системы.

## Назначение

Сервис отвечает за:

- хранение универсальных объектов и их атрибутов
- хранение справочника типов объектов
- учет наборов данных
- загрузку файлов пользователей в S3-совместимое хранилище
- хранение метаданных файлов и датасетов в реляционной БД

## Архитектурная роль

Этот сервис является источником данных для остальных микросервисов:

1. `storage-service` хранит объекты, датасеты и файлы.
2. `preprocessing-service` получает из него сырой датасет или файл.
3. `comparative-analysis-service` получает уже подготовленный датасет.

## Основные сущности

- `object_types`
- `objects`
- `object_attributes`
- `stored_files`
- `datasets`

## Основные endpoint'ы

### Типы объектов

- `GET /api/v1/object-types`
- `POST /api/v1/object-types`
- `GET /api/v1/object-types/{id}`

### Объекты

- `GET /api/v1/objects`
- `POST /api/v1/objects`
- `GET /api/v1/objects/{id}`
- `PUT /api/v1/objects/{id}`
- `DELETE /api/v1/objects/{id}`

### Файлы

- `GET /api/v1/files`
- `POST /api/v1/files/upload`
- `GET /api/v1/files/{id}`
- `GET /api/v1/files/{id}/download-url`

### Наборы данных

- `GET /api/v1/datasets`
- `POST /api/v1/datasets`

## Локальный запуск

```bash
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8070
```

## Примеры

Создать объект:

```bash
curl -X POST http://localhost:8070/api/v1/objects ^
  -H "Content-Type: application/json" ^
  --data-binary "@example-object.json"
```

Создать запись датасета:

```bash
curl -X POST http://localhost:8070/api/v1/datasets ^
  -H "Content-Type: application/json" ^
  --data-binary "@example-dataset.json"
```

Загрузить файл:

```bash
curl -X POST "http://localhost:8070/api/v1/files/upload?purpose=dataset" ^
  -F "file=@sample.csv"
```

## Что можно расширить следующим шагом

- фильтрацию объектов по атрибутам
- пакетный импорт объектов
- версионирование датасетов
- аудит изменений
- presigned upload URL вместо прямого проксирования файлов через сервис
