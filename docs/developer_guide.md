# Руководство разработчика

## 1. Назначение

Этот документ нужен как короткая рабочая памятка для разработчика системы сравнительного анализа объектов. Здесь собраны базовые сведения о структуре проекта, локальном запуске, Docker-развертывании, тестировании и типовых точках входа в код.

## 2. Технологический стек

### Backend

- Python
- FastAPI
- SQLAlchemy
- Pydantic
- pandas
- numpy
- PostgreSQL
- MinIO / S3

### Frontend

- React
- TypeScript
- Vite

### Инфраструктура

- Docker
- Docker Compose

## 3. Структура проекта

```text
backend/
  app/
    main.py
    schemas/
    services/
    db/
    core/
  tests/

frontend/
  src/
    App.tsx
    api.ts
    types.ts
    styles.css
    components/

docs/
scripts/
```

Ключевые файлы:

- [C:\Users\Stepan\Documents\New project\backend\app\main.py](C:\Users\Stepan\Documents\New project\backend\app\main.py) — основная точка входа backend.
- [C:\Users\Stepan\Documents\New project\backend\app\services\pipeline_engine.py](C:\Users\Stepan\Documents\New project\backend\app\services\pipeline_engine.py) — оркестрация аналитического конвейера.
- [C:\Users\Stepan\Documents\New project\backend\app\services\preprocessing_engine.py](C:\Users\Stepan\Documents\New project\backend\app\services\preprocessing_engine.py) — предобработка данных.
- [C:\Users\Stepan\Documents\New project\backend\app\services\analysis_engine.py](C:\Users\Stepan\Documents\New project\backend\app\services\analysis_engine.py) — аналитическое ядро.
- [C:\Users\Stepan\Documents\New project\frontend\src\App.tsx](C:\Users\Stepan\Documents\New project\frontend\src\App.tsx) — основной пользовательский workflow.
- [C:\Users\Stepan\Documents\New project\frontend\src\api.ts](C:\Users\Stepan\Documents\New project\frontend\src\api.ts) — клиентские вызовы backend API.

## 4. Локальное развертывание без Docker

### 4.1 Backend

Из корня проекта:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r backend/requirements.txt
uvicorn backend.app.main:app --reload --host 0.0.0.0 --port 8000
```

Backend по умолчанию будет доступен по адресу:

- [http://localhost:8000](http://localhost:8000)

Проверка работоспособности:

- [http://localhost:8000/health](http://localhost:8000/health)

### 4.2 Frontend

Во второй консоли:

```powershell
npm --prefix frontend install
npm --prefix frontend run dev
```

Frontend обычно доступен по адресу:

- [http://localhost:5173](http://localhost:5173)

## 5. Развертывание через Docker

Если используется подготовленный compose-стек или PowerShell-скрипты проекта, основной рабочий вариант такой:

```powershell
.\scripts\start-docker.ps1
```

Если требуется ручной запуск через Docker Compose:

```powershell
docker compose up --build
```

Что важно после изменений:

- после изменения backend API нужен rebuild/restart backend-контейнера;
- после изменения frontend нужен rebuild/restart frontend-контейнера;
- после изменения схем данных желательно пересобрать оба слоя.

## 6. Основные API-маршруты

Ключевые endpoint:

- `GET /health`
- `POST /api/v1/pipeline/upload-profile`
- `POST /api/v1/pipeline/profile-stored`
- `POST /api/v1/pipeline/preprocess-refresh`
- `POST /api/v1/pipeline/run`
- `POST /api/v1/pipeline/run-stored`
- `POST /api/v1/pipeline/raw-objects`
- `POST /api/v1/pipeline/object-search`
- `GET /api/v1/system/dashboard`

При разработке UI чаще всего используются именно:

- загрузка профиля;
- обновление предобработки;
- запуск расчета;
- получение сырых данных объекта;
- поиск объектов по всему датасету.

## 7. Тестирование

### 7.1 Backend

```powershell
.\.venv\Scripts\python.exe -m pytest -q
```

### 7.2 Frontend

```powershell
npm --prefix frontend run test
npm --prefix frontend run build
```

Рекомендуемый минимальный цикл проверки после правок:

1. `pytest`
2. `npm run build`
3. ручная проверка нужного пользовательского сценария

## 8. Где искать нужную логику

### Импорт данных

- [C:\Users\Stepan\Documents\New project\backend\app\services\import_parser.py](C:\Users\Stepan\Documents\New project\backend\app\services\import_parser.py)

### Профилирование

- [C:\Users\Stepan\Documents\New project\backend\app\services\profiling_engine.py](C:\Users\Stepan\Documents\New project\backend\app\services\profiling_engine.py)

### Единицы измерения

- [C:\Users\Stepan\Documents\New project\backend\app\services\measurement_parsing.py](C:\Users\Stepan\Documents\New project\backend\app\services\measurement_parsing.py)

### Предобработка

- [C:\Users\Stepan\Documents\New project\backend\app\services\preprocessing_engine.py](C:\Users\Stepan\Documents\New project\backend\app\services\preprocessing_engine.py)

### Аналитика и аналоговый поиск

- [C:\Users\Stepan\Documents\New project\backend\app\services\analysis_engine.py](C:\Users\Stepan\Documents\New project\backend\app\services\analysis_engine.py)
- [C:\Users\Stepan\Documents\New project\backend\app\services\pipeline_engine.py](C:\Users\Stepan\Documents\New project\backend\app\services\pipeline_engine.py)

### Пользовательский workflow

- [C:\Users\Stepan\Documents\New project\frontend\src\App.tsx](C:\Users\Stepan\Documents\New project\frontend\src\App.tsx)

## 9. Логи и диагностика

Основной backend-лог:

- [C:\Users\Stepan\Documents\New project\logs\backend.log](C:\Users\Stepan\Documents\New project\logs\backend.log)

Что обычно смотреть в первую очередь:

- `upload-profile`
- `preprocess-refresh`
- `run-stored`
- ошибки валидации
- время выполнения шагов конвейера

## 10. Практические рекомендации

- Не смешивать изменения схем backend и frontend без пересборки обоих слоев.
- После изменения структур ответа проверять и `types.ts`, и `api.ts`, и рендер в `App.tsx`.
- При работе с большими датасетами проверять не только корректность, но и время выполнения.
- Для новых пользовательских режимов сначала продумывать, какие данные нужны в API, и только потом собирать UI.
- Для строковых значений и русскоязычных надписей избегать опасных массовых перезаписей файлов, чтобы не ломать кодировку.

## 11. Краткий маршрут для нового разработчика

Если нужно быстро войти в проект, лучше идти в таком порядке:

1. Прочитать [C:\Users\Stepan\Documents\New project\docs\system_essence.md](C:\Users\Stepan\Documents\New project\docs\system_essence.md)
2. Запустить backend и frontend локально
3. Пройти весь пользовательский сценарий вручную
4. Изучить `pipeline_engine.py`, `preprocessing_engine.py`, `analysis_engine.py`
5. Затем перейти к `App.tsx` и посмотреть, как связаны этапы интерфейса

## 12. Вывод

Проект устроен как единая информационно-аналитическая система с явным разделением на импорт, подготовку данных, аналитическое ядро, результаты и историю. Для разработки важнее всего понимать не отдельные файлы, а общий конвейер прохождения данных через backend и frontend. Именно это позволяет вносить изменения без нарушения пользовательского сценария.
