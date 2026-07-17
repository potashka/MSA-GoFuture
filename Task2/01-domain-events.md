# ADR-T2-01: Каталог доменных событий

Дата: 2026-07-17
Авторы: Архитектурный комитет GoFuture, доменные команды

## Контекст

Task1 определил доменные границы сервисов и их публикуемые/потребляемые
события на уровне карты сервисов (см.
[Task1/02-service-map.md](../Task1/02-service-map.md)) и целевую диаграмму
[Task1/c2-to-be.puml](../Task1/c2-to-be.puml). Для событийной платформы,
рассчитанной на 500 тыс. конкурентных поездок и динамическое ценообразование
в реальном времени (требования П1–П4, [Task1/01-nfr.md](../Task1/01-nfr.md)),
необходим единый, зафиксированный каталог доменных событий — общий контракт
между командами, обязательный для проектирования топиков Kafka
([02-kafka-topics.md](02-kafka-topics.md)), потоковой обработки
([03-stream-processing.md](03-stream-processing.md)) и саги жизненного цикла
поездки ([04-saga.md](04-saga.md)).

Каталог ниже уточняет и заменяет неформальные имена событий из
[Task1/02-service-map.md](../Task1/02-service-map.md) (например,
`TripStarted`/`TripCompleted`/`DriverMatched`/`PriceQuoted`/`FraudCheckResult`)
на согласованный набор имён, использующийся во всех артефактах Task2:
`BookingCreated`, `BookingConfirmed`, `BookingCancelled`, `DriverAssigned`,
`DriverLocationUpdated`, `PriceCalculated`, `SurgeActivated`,
`PaymentAuthorized`, `PaymentCaptured`, `PaymentFailed`, `PayoutInitiated`,
`PayoutCompleted`, `FraudCheckCompleted`, `RideStarted`, `RideCompleted`,
`NotificationRequested`.

## Требования

- У каждого события — ровно один логический продюсер (сервис-владелец
  схемы), чтобы не возникало конкурирующих источников истины.
- Ключ партиционирования каждого события должен обеспечивать необходимый
  порядок обработки: `booking_id` — для событий саги поездки (шаги должны
  обрабатываться по порядку в рамках одной поездки), `geo_cell`/`driver_id`
  — там, где порядок важен в рамках зоны или конкретного водителя, а не
  конкретной поездки.
- Retention каждого события определяется его ролью: операционные события
  саги — короткий срок (данные фиксируются в БД сервиса-владельца, Kafka —
  лишь канал доставки), денежные и регуляторно значимые события — более
  длинный срок (требование Б3, [Task1/01-nfr.md](../Task1/01-nfr.md), и
  необходимость расследования споров).
- Событие или команда — характер взаимодействия должен быть явным: "факт"
  (что-то произошло, необратимо) или "команда" (запрос на действие,
  адресован конкретному потребителю).

## Решение

Ниже — каталог из 16 событий. Формат аналогичен карте сервисов
([Task1/02-service-map.md](../Task1/02-service-map.md)): по каждому событию —
продюсер, потребители, ключевые поля схемы, ключ партиционирования,
semantics (fact/command), retention.

### BookingCreated

- **Продюсер**: Booking Service.
- **Потребители**: Booking Service (сам оркестратор — старт саги, см.
  [04-saga.md](04-saga.md)); Fraud Service (фоновое накопление паттернов,
  независимо от блокирующей проверки саги); Geography Service (сигнал
  спроса по зоне); Analytics Service.
- **Ключевые поля схемы**: `booking_id`, `passenger_id`, `pickup_geo_cell`,
  `dropoff_geo_cell`, `region`, `city_id`, `requested_at`,
  `payment_method_id`.
- **Ключ партиционирования**: `booking_id`.
- **Semantics**: fact.
- **Retention**: 7 дней (операционный поток саги; источник истины —
  Booking DB).

### BookingConfirmed

- **Продюсер**: Booking Service (после успешного прохождения всех шагов
  саги: fraud check → pricing → driver matching → payment authorization).
- **Потребители**: Geography Service (заказ перешёл в исполнение);
  Analytics Service; Booking Service публикует вслед за этим
  `NotificationRequested` для пассажира и водителя (см. ниже).
- **Ключевые поля схемы**: `booking_id`, `driver_id`, `price_id`,
  `final_price`, `confirmed_at`, `eta_pickup`.
- **Ключ партиционирования**: `booking_id`.
- **Semantics**: fact.
- **Retention**: 7 дней.

### BookingCancelled

- **Продюсер**: Booking Service (по инициативе пассажира/водителя, либо
  как компенсирующее событие саги при отказе одного из шагов, см.
  [04-saga.md](04-saga.md)).
- **Потребители**: Driver Service (освобождение водителя, возврат в пул);
  Payments Service (отмена холда, если он уже был выполнен); Fraud Service
  (сигнал для скоринга частых отмен); Analytics Service.
- **Ключевые поля схемы**: `booking_id`, `cancelled_by`
  (`passenger`/`driver`/`saga-compensation`), `reason_code`, `cancelled_at`,
  `saga_step_failed` (опционально, если отмена — компенсация).
- **Ключ партиционирования**: `booking_id`.
- **Semantics**: fact.
- **Retention**: 14 дней (используется также для расследований и разбора
  инцидентов саги).

### DriverAssigned

- **Продюсер**: Driver Service (результат "умного" подбора водителя, а не
  просто ближайшего, см. [Task1/02-service-map.md](../Task1/02-service-map.md)).
- **Потребители**: Booking Service (оркестратор — прогресс саги);
  Geography Service (обновление доступности в зоне); Analytics Service.
- **Ключевые поля схемы**: `booking_id`, `driver_id`, `assigned_at`,
  `driver_geo_cell`, `eta_to_pickup`, `match_reason` (например,
  `smart-reposition`/`nearest-fallback`).
- **Ключ партиционирования**: `booking_id` (порядок в рамках саги поездки).
- **Semantics**: fact.
- **Retention**: 7 дней.

### DriverLocationUpdated

- **Продюсер**: Driver Service (приём высокочастотных пингов от
  приложения водителя, латентность согласно требованию П3,
  [Task1/01-nfr.md](../Task1/01-nfr.md)).
- **Потребители**: Geography Service (агрегация плотности водителей по
  зонам); Pricing/Flink (сигнал предложения для surge, см.
  [03-stream-processing.md](03-stream-processing.md)); Booking Service
  (обновление ETA для активных поездок); Analytics Service (сэмплированно).
- **Ключевые поля схемы**: `driver_id`, `geo_cell`, `lat`, `lon`, `speed`,
  `heading`, `status` (`available`/`busy`/`offline`), `updated_at`.
- **Ключ партиционирования**: `geo_cell` (не `booking_id` — событие не
  привязано к конкретной поездке; партиционирование по зоне даёт локальность
  для оконных агрегаций в Flink, см. [02-kafka-topics.md](02-kafka-topics.md)).
- **Semantics**: fact (по характеру — телеметрия состояния, естественно
  сжимаемая по ключу `driver_id` при log compaction).
- **Retention**: короткий (единицы часов) + log compaction по `driver_id`
  для хранения только последнего известного состояния (см.
  [02-kafka-topics.md](02-kafka-topics.md)).

### PriceCalculated

- **Продюсер**: Pricing Service.
- **Потребители**: Booking Service (оркестратор — сумма для авторизации
  платежа); Analytics Service.
- **Ключевые поля схемы**: `booking_id`, `base_fare`, `surge_multiplier`,
  `final_price`, `currency`, `pricing_geo_cell`, `calculated_at`.
- **Ключ партиционирования**: `booking_id`.
- **Semantics**: fact.
- **Retention**: 7 дней.

### SurgeActivated

- **Продюсер**: Pricing Service (по результатам оконных агрегаций Flink
  спроса/предложения по `geo_cell`, см.
  [03-stream-processing.md](03-stream-processing.md)).
- **Потребители**: Driver Service ("умное" перераспределение — стимул
  водителям ехать в зону с повышенным спросом); Geography Service
  (координация борьбы с "горячими зонами"); Booking Service публикует
  `NotificationRequested` для водителей в затронутой зоне; Analytics
  Service.
- **Ключевые поля схемы**: `geo_cell`, `zone_id`, `surge_multiplier`,
  `demand_supply_ratio`, `activated_at`, `window_expires_at`.
- **Ключ партиционирования**: `geo_cell`.
- **Semantics**: fact (изменение состояния зоны, не команда).
- **Retention**: 24 часа + log compaction по `geo_cell` для последнего
  актуального состояния зоны.

### PaymentAuthorized

- **Продюсер**: Payments Service.
- **Потребители**: Booking Service (оркестратор — последний шаг саги перед
  `BookingConfirmed`); Analytics Service.
- **Ключевые поля схемы**: `booking_id`, `payment_id`, `amount`,
  `currency`, `payment_method_id`, `authorized_at`.
- **Ключ партиционирования**: `booking_id`.
- **Semantics**: fact.
- **Retention**: 30 дней (денежное событие — расширенное окно для
  расследований и споров).

### PaymentCaptured

- **Продюсер**: Payments Service (после `RideCompleted` — фактическое
  списание ранее авторизованной суммы).
- **Потребители**: Payouts Service (начисление заработка водителю);
  Analytics Service; Booking Service публикует `NotificationRequested`
  (чек/квитанция пассажиру).
- **Ключевые поля схемы**: `booking_id`, `payment_id`, `captured_amount`,
  `captured_at`.
- **Ключ партиционирования**: `booking_id`.
- **Semantics**: fact.
- **Retention**: 30 дней.

### PaymentFailed

- **Продюсер**: Payments Service.
- **Потребители**: Booking Service (оркестратор — триггер компенсации
  саги, см. [04-saga.md](04-saga.md)); Fraud Service (сигнал риска —
  повторные отказы платежа); Analytics Service.
- **Ключевые поля схемы**: `booking_id`, `payment_id`, `failure_reason`,
  `failure_code`, `failed_at`.
- **Ключ партиционирования**: `booking_id`.
- **Semantics**: fact.
- **Retention**: 30 дней.

### PayoutInitiated

- **Продюсер**: Payouts Service.
- **Потребители**: Analytics Service; Booking Service публикует
  `NotificationRequested` для водителя.
- **Ключевые поля схемы**: `payout_id`, `driver_id`, `amount`, `currency`,
  `period_start`, `period_end`, `initiated_at`.
- **Ключ партиционирования**: `driver_id` (порядок начислений в рамках
  одного водителя важнее порядка в рамках отдельной поездки).
- **Semantics**: fact.
- **Retention**: 90 дней (бухгалтерский/регуляторный контур, см.
  [Task1/02-service-map.md](../Task1/02-service-map.md) про иные
  регуляторные требования Payouts).

### PayoutCompleted

- **Продюсер**: Payouts Service.
- **Потребители**: Analytics Service; уведомление водителя через
  `NotificationRequested`.
- **Ключевые поля схемы**: `payout_id`, `driver_id`, `amount`,
  `bank_reference`, `completed_at`.
- **Ключ партиционирования**: `driver_id`.
- **Semantics**: fact.
- **Retention**: 90 дней.

### FraudCheckCompleted

- **Продюсер**: Fraud Service.
- **Потребители**: Booking Service (оркестратор — вердикт
  `allow`/`block`/`review`, шаг саги); Analytics Service.
- **Ключевые поля схемы**: `booking_id`, `check_id`, `verdict`,
  `risk_score`, `triggered_rules`, `completed_at`.
- **Ключ партиционирования**: `booking_id`.
- **Semantics**: fact.
- **Retention**: 30 дней (нужно дольше стандартного окна для расследования
  инцидентов).

### RideStarted

- **Продюсер**: Booking Service (владелец жизненного цикла поездки, см.
  [Task1/02-service-map.md](../Task1/02-service-map.md)).
- **Потребители**: Geography Service (снятие водителя из доступного пула
  зоны); Analytics Service; уведомление через `NotificationRequested`.
- **Ключевые поля схемы**: `booking_id`, `driver_id`, `started_at`,
  `start_geo_cell`.
- **Ключ партиционирования**: `booking_id`.
- **Semantics**: fact.
- **Retention**: 7 дней.

### RideCompleted

- **Продюсер**: Booking Service.
- **Потребители**: Payments Service (инициирует `PaymentCaptured`);
  Payouts Service (начисление после `PaymentCaptured`); Driver Service
  (освобождение водителя в пул); Fraud Service (пост-анализ поездки);
  Analytics Service; уведомление через `NotificationRequested`.
- **Ключевые поля схемы**: `booking_id`, `driver_id`, `completed_at`,
  `end_geo_cell`, `distance_km`, `duration_sec`, `final_price_ref`.
- **Ключ партиционирования**: `booking_id`.
- **Semantics**: fact.
- **Retention**: 30 дней (от него зависят Payments/Payouts и возможные
  споры — расширенное окно).

### NotificationRequested

- **Продюсер**: любой доменный сервис, которому требуется уведомить
  пользователя (Booking, Payments, Payouts, Pricing/Surge — для
  водителей, Driver) — единая явная команда вместо подписки Notification
  Service на весь поток доменных событий.
- **Потребители**: Notification Service (единственный потребитель).
- **Ключевые поля схемы**: `notification_id`, `recipient_type`
  (`passenger`/`driver`), `recipient_id`, `template_code`, `payload`
  (параметры шаблона), `channel_hint`, `correlation_id` (`booking_id` /
  `payout_id` и т. п.), `requested_at`.
- **Ключ партиционирования**: `recipient_id` (сохраняет порядок
  уведомлений одному получателю).
- **Semantics**: command (запрос на действие "отправь уведомление", а не
  факт свершившегося события).
- **Retention**: 3 дня (операционная очередь доставки; журнал и статусы
  доставки хранятся в Notification DB, а не в Kafka).

## Альтернативы

- **Оставить Notification Service подписанным на весь поток доменных
  событий** (как описано в AS-IS-неформальной модели
  [Task1/02-service-map.md](../Task1/02-service-map.md)). Отклонено:
  создаёт широкую неявную связанность — любое изменение схемы любого
  события потенциально ломает Notification. Явная команда
  `NotificationRequested`, публикуемая продюсером с уже готовым
  содержимым, устраняет эту связанность и явно поименована как компромисс
  в риске Task1 ("Fraud и Notification как потребители всего потока
  событий создают широкую неявную связанность", см.
  [Task1/02-service-map.md](../Task1/02-service-map.md)).
- **Единый ключ партиционирования (`booking_id`) для всех событий, включая
  `DriverLocationUpdated`.** Отклонено: локация не привязана к конкретной
  поездке (водитель не в поездке тоже шлёт локацию) и требует
  партиционирования по `geo_cell` для эффективных оконных агрегаций в
  Flink и равномерного распределения нагрузки (см.
  [02-kafka-topics.md](02-kafka-topics.md)).
- **Публиковать команды шагов саги (`FraudCheckRequested`,
  `PriceCalculationRequested` и т. п.) как самостоятельные каталогизируемые
  доменные события.** Отклонено на уровне этого каталога: команды,
  которыми оркестратор инициирует каждый шаг, — это адресные вызовы
  "точка-точка" (синхронный вызов или выделенный командный канал, см.
  [04-saga.md](04-saga.md)), а не широковещательные факты; в каталог
  включены только результирующие факты каждого шага, которые действительно
  представляют интерес для нескольких потребителей.

## Компромиссы и риски

- `NotificationRequested` как единая точка входа Notification Service
  создаёт зависимость всех продюсеров от единого формата команды — требует
  дисциплины Schema Registry и contract-тестов (см.
  [02-kafka-topics.md](02-kafka-topics.md),
  [05-delivery-guarantees.md](05-delivery-guarantees.md)).
- `DriverLocationUpdated` — высокочастотный поток; риск перегрузки
  партиций/потребителей при плохо подобранном ключе или числе партиций;
  требует отдельного топика с укороченным retention и большим числом
  партиций (см. [02-kafka-topics.md](02-kafka-topics.md)).
- Денежные события (`Payment*`, `Payout*`) имеют более долгий retention
  (30–90 дней) — увеличивает объём хранения Kafka, требует явной политики
  размера диска/tiered storage на кластере.
- Порядок событий жизненного цикла поездки (`BookingCreated` →
  `FraudCheckCompleted` → `PriceCalculated` → `DriverAssigned` →
  `PaymentAuthorized` → `BookingConfirmed` → `RideStarted` →
  `RideCompleted` → `PaymentCaptured` → `PayoutInitiated` →
  `PayoutCompleted`) не должен нарушаться; корректность порядка
  обеспечивается конструкцией саги (оркестратор — единственный, кто решает,
  когда переходить к следующему шагу), а не самой Kafka (см.
  [04-saga.md](04-saga.md)).

## Статус

Предложено.
