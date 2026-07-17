# Task4 — Платформа данных и ML GoFuture

Артефакты дизайна единой платформы сбора, обработки и анализа данных из
всех микросервисов для ML-моделей (динамическое ценообразование,
прогнозирование спроса, обнаружение мошенничества) и интеграции в BI.
Развивает событийную платформу Task2 (каталог событий, Kafka) и глобальную
модель регионов Task3 (резидентность данных, региональный шардинг).
Исходный контекст — [docs/context.md](../docs/context.md), шаблон ADR —
[docs/adr-template.md](../docs/adr-template.md).

## Содержание

| Файл | Описание |
|---|---|
| [01-data-pipeline.md](01-data-pipeline.md) | ADR архитектуры пайплайна: источники (Kafka-события + CDC), региональная обработка Flink, ClickHouse + Data Lake (S3), анонимизирующий экспорт для кросс-регионального BI/ML, Feature Store (онлайн+офлайн), ML-платформа (training, model registry, model serving), петля обратной связи, привязка к резидентности данных. |
| [c4-data-pipeline.puml](c4-data-pipeline.puml) | Диаграмма контейнеров (C4): доменные сервисы → Kafka/CDC → Flink → ClickHouse + Data Lake → Feature Store → ML-платформа → обратно в Pricing/Fraud; ветка BI: ClickHouse → DataLens → корпоративный менеджер и бухгалтер. Проверена локальной компиляцией PlantUML без ошибок. |
| [02-data-quality.md](02-data-quality.md) | ADR механизмов качества данных: Schema Registry, data contracts, валидация в пайплайне (Great Expectations/dbt tests), DQ-метрики в Prometheus/Grafana, карантинная зона, data lineage (OpenLineage), мониторинг дрейфа данных для ML — для каждого указан этап пайплайна и закрываемая проблема. |

## Как читать

[01-data-pipeline.md](01-data-pipeline.md) и
[c4-data-pipeline.puml](c4-data-pipeline.puml) задают саму архитектуру
пайплайна и ML-платформы; [02-data-quality.md](02-data-quality.md)
описывает, как гарантируется, что данные, текущие через эту архитектуру,
можно доверять — оба документа рассчитаны на совместное чтение, а не
последовательно независимое.

## Закрытый открытый вопрос из предыдущих задач

[Task2/02-kafka-topics.md](../Task2/02-kafka-topics.md) и
[Task3/02-replication.md](../Task3/02-replication.md) явно оставляли
открытым вопрос: как Analytics получает глобальный срез данных, не
нарушая региональный шардинг и резидентность. Task4 закрывает этот вопрос
явным механизмом anonymization/aggregation export
([01-data-pipeline.md](01-data-pipeline.md)) — только агрегированные и
анонимизированные данные когда-либо реплицируются в аналитический
(домашний) регион `core`.
