# ADR-T2-06: Мониторинг событийной платформы

Дата: 2026-07-17
Авторы: Архитектурный комитет GoFuture, SRE, платформенная команда

## Контекст

Событийная платформа (Kafka, Flink, оркестратор саги в Booking Service,
DLQ, Schema Registry — см. [c2-event-platform.puml](c2-event-platform.puml))
вносит классы отказов, не покрываемые типовым мониторингом монолита:
отставание потребителей (consumer lag), накопление необработанных
сообщений (DLQ), деградацию сквозной латентности многошаговой Saga
([04-saga.md](04-saga.md)), ошибки outbox-relay и деградацию stream
processing. У компании уже есть эксплуатируемый стек наблюдаемости —
Prometheus, Grafana, Loki, Alertmanager (см. [docs/context.md](../docs/context.md)).
Нужно расширить его под событийную платформу и зафиксировать конкретный
набор инструментов, метрик, SLI/SLO и алертов.

## Требования

- MTTR критических сервисов < 1 часа (требование Э1,
  [Task1/01-nfr.md](../Task1/01-nfr.md)) требует единого места диагностики
  инцидента, а не переключения между несколькими независимыми системами
  мониторинга.
- Наблюдаемость состояния каждой из 500 тыс. одновременных поездок
  (обоснование выбора оркестрации, [04-saga.md](04-saga.md)) требует
  сквозного трейсинга через границы сервисов и Kafka, а не только точечных
  метрик по каждому сервису отдельно.
- SRE и платформенная команда уже обучены существующему стеку — его
  следует переиспользовать и расширять, а не заменять принципиально другим
  набором инструментов ради событийной платформы.
- Отказ или деградация обработки событий (лаг, DLQ, backpressure, stuck
  Saga) должны обнаруживаться проактивно алертом, а не постфактум по
  жалобам пользователей.
- Метрики и labels не должны содержать PII: в labels допускаются
  технические и бизнес-идентификаторы корреляции, но не телефоны, email,
  полные координаты, платёжные реквизиты или имена пользователей.

## Решение

### Инструменты

Переиспользуется текущий стек Prometheus, Grafana, Loki, Alertmanager и
добавляются компоненты, отражённые на C2-диаграмме:

| Инструмент | Роль |
|---|---|
| OpenTelemetry Collector | Принимает OTLP-метрики, логи/корреляционные атрибуты и трейсы от сервисов, Booking Saga Orchestrator и Flink jobs; маршрутизирует метрики в Prometheus, трейсы в Tempo, логи/корреляцию в Loki. |
| Prometheus | Собирает и хранит time-series метрики сервисов, Kafka, Flink, outbox/DLQ и Saga. |
| Grafana | Единый UI для дашбордов, алертов, логов и трейсов. |
| Loki | Хранит структурированные логи сервисов и платформенных компонентов с корреляцией по `trace_id`, `saga_id`, `booking_id`. |
| Alertmanager | Дедуплицирует, группирует и маршрутизирует алерты SRE/дежурным командам. |
| Tempo | Хранит распределённые трейсы Saga и Kafka processing spans. |
| Kafka Exporter | Экспортирует consumer lag, состояние брокеров, ISR, partitions, latency и throughput Kafka. |
| Flink metrics/exporter | Экспортирует метрики Flink jobs: checkpoints, backpressure, watermarks, state и failures. |

### Технические метрики

| Область | Метрики |
|---|---|
| Сервисы | RED: request rate, error rate, duration p50/p95/p99; saturation CPU/memory/thread pools; ошибки outbox relay. |
| Kafka brokers | `under_replicated_partitions`, ISR shrink/expand rate, broker disk usage, offline partitions, controller changes, produce latency, fetch latency, bytes/messages in/out. |
| Kafka consumers | consumer lag в сообщениях и времени по topic/partition/group, rebalance count, commit latency, failed deserialization count. |
| Schema Registry | availability, request latency, schema compatibility failures, registry error rate. |
| Outbox | outbox lag от записи в БД до публикации в Kafka, размер backlog, relay error rate, возраст самого старого outbox-сообщения. |
| DLQ | DLQ growth, возраст самого старого сообщения, число сообщений по topic/group/error_class. |
| Flink | checkpoint duration, failed checkpoints, checkpoint alignment time, backpressure, watermark lag, state size, restart count, records in/out, processing latency. |
| DriverLocationUpdated | `deduplicated_events_total`, `stale_sequence_dropped_total`, `watermark_lag_ms`, state TTL expirations, state size по `driver_id` и `geo_cell`. |

### Бизнес-метрики

| Область | Метрики |
|---|---|
| Booking Saga | Saga duration end-to-end и по шагам, Saga timeout rate, compensation rate, stuck Saga count, число Saga по состояниям. |
| Конверсия бронирования | `BookingCreated` → `BookingConfirmed` / `BookingCancelled`, доля отказов по `saga_step_failed`, причины отмен. |
| Driver matching | доля `DriverReservationFailed`, время до `DriverReserved`, доля освобождений водителя после компенсации. |
| Payments | доля `PaymentAuthorizationFailed`, время авторизации, число release/void операций, ручные проверки платежей. |
| Surge | количество `SurgeActivated`, длительность активного surge по `geo_cell`, отклонение спрос/предложение. |

### SLI/SLO

| SLI | Целевой SLO |
|---|---|
| Доступность Kafka produce/fetch для Tier-1 топиков | Соответствует доступности Tier-1 пути создания заказа из [Task1/01-nfr.md](../Task1/01-nfr.md); плановый простой для Tier-1 не допускается. |
| Consumer freshness для Saga-топиков | p95 задержки доставки и обработки результата шага укладывается в таймаут шага из [04-saga.md](04-saga.md). |
| Saga completion latency | p95 happy path укладывается в бюджет П2; p99 контролируется отдельным алертом деградации. |
| Outbox publish latency | p95 от commit локальной транзакции до публикации в Kafka не превышает операционный бюджет шага Saga. |
| DLQ freshness | Возраст старейшего сообщения в DLQ ниже порога разбора; рост DLQ не должен оставаться без алерта. |
| Flink freshness | Watermark lag и checkpoint duration не приводят к устаревшим `SurgeActivated`, влияющим на pricing. |
| Schema Registry availability | Доступность достаточна для producer/consumer serializers; деградация не должна блокировать rolling deploy схем. |

### Алерты

| Алерт | Условие | Действие |
|---|---|---|
| Kafka consumer lag high | Лаг Saga или платежной consumer group выше порога по времени/сообщениям. | SRE + команда-владелец consumer group. |
| Under-replicated partitions | `under_replicated_partitions > 0` дольше короткого окна. | SRE проверяет брокеры, ISR и диск. |
| ISR shrink spike | Резкий рост ISR shrink rate. | SRE проверяет сетевые/дисковые деградации брокеров. |
| Broker disk high | Broker disk usage выше порога. | SRE включает capacity/runbook, проверяет retention и tiered storage. |
| Produce/fetch latency high | p95/p99 produce или fetch latency выше SLO. | SRE + платформенная команда. |
| Consumer rebalance storm | Rebalance count выше нормы. | Команда consumer group проверяет autoscaling, max.poll и deploy. |
| Flink checkpoints failing | Failed checkpoints или checkpoint duration выше SLO. | Data/platform team проверяет state backend и backpressure. |
| Flink backpressure high | Backpressure держится выше порога. | Масштабирование job или расследование downstream. |
| Watermark lag high | Watermark lag влияет на freshness surge. | Pricing/Data team проверяет задержки input и watermark strategy. |
| Outbox lag high | Возраст старого outbox-сообщения выше бюджета. | Команда сервиса и SRE проверяют relay/CDC. |
| DLQ growth | DLQ растёт или старейшее сообщение старше порога. | Владелец consumer group разбирает poison messages. |
| Saga timeout rate high | Доля `SagaStepTimedOut` выше baseline. | Booking team и владелец деградирующего шага. |
| Compensation rate high | Компенсации растут выше baseline. | Booking/Driver/Payments triage. |
| Stuck Saga count high | `STUCK_REQUIRES_MANUAL_RESOLUTION` выше порога. | Операторы и доменная команда разбирают вручную. |
| Schema Registry unavailable | Ошибки availability/latency или рост compatibility failures. | Платформенная команда. |

### Трейсинг и labels

Базовые trace attributes для Saga и Kafka processing:

- `trace_id`;
- `saga_id`;
- `booking_id`;
- `region_id`;
- `tenant_id`, если включена мультитенантность;
- `event_type`, `event_id`, `correlation_id`, `causation_id`;
- `kafka.topic`, `kafka.partition`, `kafka.offset`, `consumer_group`.

PII не помещается в labels и trace attributes высокой кардинальности:
нельзя писать телефон, email, имя, точный адрес, полные координаты,
платёжные реквизиты или содержимое уведомлений. Для поиска инцидентов
используются `booking_id`, `saga_id`, `trace_id`, `region_id` и
`tenant_id`.

### Обоснование расширения существующего стека вместо замены

- Prometheus/Grafana/Loki/Alertmanager уже эксплуатируются SRE и знакомы
  командам; замена потребовала бы параллельной миграции дашбордов и
  алертов в разгар декомпозиции монолита.
- Kafka Exporter, Flink metrics/exporter, OpenTelemetry Collector и Tempo
  нативно совместимы с текущим стеком и дают единый UI в Grafana для
  метрик, логов и трейсов.
- MTTR < 1 часа (Э1) требует единого места диагностики: отдельный,
  несовместимый стек трейсинга под событийную платформу увеличил бы время
  переключения контекста при инциденте.

## Альтернативы

- **Отдельный специализированный APM/мониторинг под событийную платформу**
  (вендорское решение для Kafka/потоковой обработки, не интегрированное с
  текущим стеком). Отклонено: создаёт второй независимый источник истины
  для инцидентов, увеличивает MTTR и не переиспользует уже настроенные
  дашборды и алерты Grafana/Alertmanager.
- **Трейсинг только через логи (Loki), без выделенного backend'а трейсов
  (Tempo).** Отклонено: без модели трейсов со spans невозможно наглядно
  визуализировать сквозную латентность Saga по шагам.
- **Инструментирование трейсинга вручную под конкретный backend, без
  OpenTelemetry Collector.** Отклонено: OTel Collector даёт единый pipeline
  сбора для метрик и трейсов и позволяет менять backend без
  переинструментирования сервисов.

## Компромиссы и риски

- Kafka Exporter, Flink metrics/exporter и OTel Collector требуют
  собственного деплоя и мониторинга доступности ("кто мониторит
  мониторинг"). Они должны быть включены в стандартный процесс деплоя и
  покрыты базовыми алертами.
- Полный сквозной трейсинг через оркестратор, Kafka и доменные сервисы в
  масштабе 500 тыс. конкурентных поездок создаёт большой объём данных —
  нужна политика sampling: 100% для ошибок, таймаутов и stuck Saga,
  частичный sampling для happy path.
- Consumer lag — необходимый, но не достаточный сигнал: низкий лаг не
  гарантирует корректность обработки. Он дополняется бизнес-сверкой
  количества `BookingCreated`, `BookingConfirmed`, `BookingCancelled` и
  числом stuck Saga.
- Региональная изоляция Kafka-кластеров ([02-kafka-topics.md](02-kafka-topics.md))
  означает, что мониторинг тоже частично региональный. Требуется федерация
  метрик или `remote_write` из региональных Prometheus для глобального
  обзора в Grafana.

## Статус

Предложено.
