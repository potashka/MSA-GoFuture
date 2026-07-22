# Task4 — Платформа данных и ML GoFuture

Артефакты дизайна единой платформы сбора, обработки и анализа данных из
всех микросервисов для ML-моделей (динамическое ценообразование,
прогнозирование спроса, обнаружение мошенничества) и интеграции в BI.
Развивает событийную платформу Task2 (каталог событий, Kafka) и глобальную
модель регионов Task3 (privacy policy, региональный шардинг).
Исходный контекст — [docs/context.md](../docs/context.md), шаблон ADR —
[docs/adr-template.md](../docs/adr-template.md).

## Содержание

| Файл | Описание |
|---|---|
| [01-data-pipeline.md](01-data-pipeline.md) | ADR архитектуры пайплайна: источники (Kafka-события + CDC), региональная обработка Flink, raw restricted zone, PII mapping/vault, pseudonymized operational analytics, anonymized aggregated datasets, Feature Store (онлайн+офлайн), ML-платформа (training, model registry, model serving), петля обратной связи, privacy flow и привязка к Task3 compliance policy. |
| [c4-data-pipeline.puml](c4-data-pipeline.puml) | Диаграмма контейнеров (C4): доменные сервисы → Kafka/CDC → data contracts/quality gate → raw restricted zone/quarantine/privacy transform → curated/anonymized serving layer → Feature Store/ML и DataLens. Проверена локальной компиляцией PlantUML без ошибок. |
| [02-data-quality.md](02-data-quality.md) | ADR механизмов качества данных: Schema Registry, data contracts, таблица DQ dimensions, валидация в пайплайне (Great Expectations/dbt tests), DQ-метрики, quality score, SLA витрин, quarantine/DLQ, replay, data catalog/lineage (OpenLineage), drift monitoring и ML data/version governance. |

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
соблюдая региональный шардинг и privacy policy. Task4 закрывает этот
вопрос явным privacy flow ([01-data-pipeline.md](01-data-pipeline.md)):
raw restricted zone остаётся региональной, pseudonymized operational
analytics используется под контролем доступа, а в аналитический
(домашний) регион `core` по базовой модели реплицируются только
anonymized aggregated datasets.

## Скриншоты диаграмм

Готовый PNG-рендер [c4-data-pipeline.puml](c4-data-pipeline.puml) — в
[../renders/Task4/](../renders/Task4/).
