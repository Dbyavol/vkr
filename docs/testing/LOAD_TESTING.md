# Нагрузочное Тестирование

В проекте реализованы два режима нагрузочного тестирования.

## 1. Internal Benchmark

Режим `internal` измеряет производительность аналитического конвейера без сетевого слоя.  
Он вызывает backend-логику напрямую и позволяет оценивать:

- время построения профиля датасета;
- время выполнения предобработки и сравнительного анализа;
- полное время выполнения сценария;
- стабильность времени при росте размера датасета и конкуренции.

Пример запуска одиночного сценария:

```powershell
.\.venv\Scripts\python.exe .\scripts\load_test_pipeline.py --mode internal --rows 5000 --iterations 5 --concurrency 1
```

Пример матричного запуска:

```powershell
.\.venv\Scripts\python.exe .\scripts\load_test_pipeline.py --mode internal --rows-list 1000,5000,10000 --concurrency-list 1,2,4 --iterations 3 --output docs\testing\load_test_internal_matrix.json
```

## 2. HTTP Pipeline Benchmark

Режим `http_pipeline` тестирует систему через реальный HTTP API поднятого backend.  
Для каждого прогона выполняются:

1. `POST /api/v1/pipeline/upload-profile`
2. `POST /api/v1/pipeline/run-stored`

Это позволяет оценить:

- задержку загрузки и первичного профилирования;
- задержку полного аналитического расчета через API;
- итоговое время пользовательского сценария;
- влияние конкуренции запросов.

Пример запуска:

```powershell
.\.venv\Scripts\python.exe .\scripts\load_test_pipeline.py --mode http_pipeline --base-url http://localhost:8050 --rows-list 1000,5000 --concurrency-list 1,2 --iterations 3 --output docs\testing\load_test_http_matrix.json
```

## Формат Результатов

Скрипт сохраняет:

- параметры запуска;
- список тестовых случаев;
- усредненные метрики;
- `min`, `max`, `p95`, `p99`;
- долю ошибок;
- детальные результаты каждого прогона.

## Интерпретация

- Для `internal` режима основной интерес представляют `profile`, `pipeline` и `total`.
- Для `http_pipeline` режима отдельно анализируются `upload`, `run` и `total`.
- При сравнении оптимизаций следует использовать одинаковые значения `rows`, `iterations` и `concurrency`.
- Для диплома рекомендуется приводить не один прогон, а матрицу не менее чем по 3 размерам датасета и 2 уровням конкуренции.
