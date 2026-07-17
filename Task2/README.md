# Task2 — Событийная платформа GoFuture

Артефакты проектирования событийной платформы для 500 тыс. конкурентных
поездок и динамического ценообразования в реальном времени. Развивает карту
сервисов и целевую архитектуру, определённые в [Task1](../Task1/README.md)
(в частности [Task1/02-service-map.md](../Task1/02-service-map.md) и
[Task1/c2-to-be.puml](../Task1/c2-to-be.puml)). Исходный контекст — в
[docs/context.md](../docs/context.md), шаблон ADR — в
[docs/adr-template.md](../docs/adr-template.md).

## Содержание

| Файл | Описание |
|---|---|
| [01-domain-events.md](01-domain-events.md) | Каталог из 16 доменных событий (ADR): продюсер, потребители, ключевые поля схемы, ключ партиционирования, semantics (fact/command), retention для каждого события. |
| [02-kafka-topics.md](02-kafka-topics.md) | ADR о схеме топиков Kafka: нейминг `{region}.{domain}.{event}`, региональная изоляция как первый уровень партиционирования, ключи партиций, число партиций/replication factor/retention по группам топиков, отдельная схема для высокочастотного `driver.location.updated`, Schema Registry (Avro, BACKWARD compatibility). |
| [03-stream-processing.md](03-stream-processing.md) | ADR о выборе инструментов потоковой обработки: Flink — для оконных агрегаций спроса/предложения (surge) и "умного" перераспределения водителей; Kafka Streams — для простых проекций и обогащения внутри сервисов. |
| [04-saga.md](04-saga.md) | ADR о выборе оркестрации (Booking Service как оркестратор) для саги бронирования поездки: полная последовательность happy path и таблица компенсирующих действий для отказа каждого шага (fraud check, pricing, driver matching, payment authorization). |
| [05-delivery-guarantees.md](05-delivery-guarantees.md) | ADR о надёжной доставке: transactional outbox у всех продюсеров, at-least-once + идемпотентные консьюмеры (dedup по `event_id`), DLQ с алертами, retry с exponential backoff, эволюция схем через Schema Registry. |
| [06-monitoring.md](06-monitoring.md) | ADR о подходе к мониторингу: расширение существующего стека (Prometheus/Grafana/Loki/Alertmanager) экспортёром Kafka, OpenTelemetry Collector и Tempo; список метрик (RED, consumer lag, DLQ, latency саги, пропускная способность топиков, трейсинг). |
| [c2-event-platform.puml](c2-event-platform.puml) | Диаграмма контейнеров C2 (C4-PlantUML) событийной платформы: доменные сервисы, Kafka-кластер с ключевыми топиками, Schema Registry, Flink, оркестратор саги, DLQ, компоненты мониторинга. Проверена локальной компиляцией PlantUML без ошибок. |

## Как читать

[01-domain-events.md](01-domain-events.md) фиксирует словарь событий →
[02-kafka-topics.md](02-kafka-topics.md) определяет их физическую
организацию в Kafka → [03-stream-processing.md](03-stream-processing.md)
описывает, как события обрабатываются в реальном времени для
ценообразования и матчинга → [04-saga.md](04-saga.md) описывает, как
события используются для согласованного выполнения бизнес-процесса
бронирования → [05-delivery-guarantees.md](05-delivery-guarantees.md) и
[06-monitoring.md](06-monitoring.md) описывают, как обеспечивается
надёжность и наблюдаемость всего этого на практике.
[c2-event-platform.puml](c2-event-platform.puml) даёт единую визуальную
сводку архитектуры.

## Согласованность с Task1

Каталог событий ([01-domain-events.md](01-domain-events.md)) уточняет
неформальные имена событий из
[Task1/02-service-map.md](../Task1/02-service-map.md) до согласованного
набора, используемого во всех артефактах Task2 — при дальнейшей работе с
[Task1/c2-to-be.puml](../Task1/c2-to-be.puml) имена событий на нём следует
свести к этому каталогу.
