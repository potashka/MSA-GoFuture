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

Операционные команды и результатные события асинхронной Saga
(`DriverReservationRequested`, `DriverReserved`, `PriceLockRequested`,
`PriceLocked`, `PaymentAuthorizationRequested`,
`PaymentAuthorizationFailed` и др.) описаны отдельно в
[04-saga.md](04-saga.md). Этот каталог фиксирует публичные доменные факты и
команду Notification, которые могут потребляться за пределами Saga.

## Требования

- У каждого fact-события — ровно один логический продюсер (сервис-владелец
  факта), чтобы не возникало конкурирующих источников истины. Для command-
  событий владелец схемы фиксируется отдельно от допустимых producers:
  команду могут публиковать несколько доменов, но только по единому
  контракту владельца схемы.
- Ключ партиционирования каждого события должен обеспечивать необходимый
  порядок обработки: `booking_id` — для событий саги поездки (шаги должны
  обрабатываться по порядку в рамках одной поездки), `driver_id` или
  `region_id:driver_id` — для локации водителя, `geo_cell` — для событий
  состояния зоны.
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
- **Потребители**: Geography Service (обновление доступности в зоне);
  Analytics Service. Booking Service может использовать событие для
  read-model/audit, но переход Saga выполняется по результатному событию
  `DriverReserved` из [04-saga.md](04-saga.md), а не по `DriverAssigned`.
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
- **Ключевые поля схемы**: envelope: `event_id`, `event_type`,
  `event_version`, `occurred_at`, `region_id`, `tenant_id` (если включена
  мультитенантность), `sequence_number`; payload: `driver_id`, `geo_cell`,
  `latitude`/`longitude` либо безопасное представление координат
  (например, H3/S2 cell + сниженная точность), `speed`, `heading`, `status`
  (`available`/`busy`/`offline`).
- **Пример envelope/payload**:

  ```json
  {
    "event_id": "01JZ4V1QY3N9S7K5E0Z8H2M4CN",
    "event_type": "DriverLocationUpdated",
    "event_version": 2,
    "occurred_at": "2026-07-17T10:15:30.123Z",
    "region_id": "sea",
    "tenant_id": "gofuture",
    "payload": {
      "driver_id": "drv_42",
      "geo_cell": "h3_8_886520d9bfffff",
      "latitude": 1.3521,
      "longitude": 103.8198,
      "speed": 32.4,
      "heading": 87,
      "status": "available",
      "sequence_number": 184233
    }
  }
  ```

- **Ключ партиционирования / Kafka record key**: `region_id:driver_id` в
  общем регионально-неймспейсированном топике либо `driver_id` внутри
  отдельного регионального топика, например `sea.driver.location.updated`.
  `geo_cell` хранится в payload и не используется как Kafka record key для
  этого события.
- **Semantics**: fact (по характеру — телеметрия состояния, естественно
  сжимаемая по record key водителя при log compaction).
- **Порядок и группировка**: Kafka сохраняет порядок обновлений одного
  водителя, потому что все события с одним `region_id:driver_id` попадают в
  одну партицию. Flink после чтения выполняет перегруппировку
  `keyBy(event.payload.geo_cell)` для оконных агрегаций по ячейкам.
- **Retention**: короткий (единицы часов) + log compaction по record key
  `region_id:driver_id`/`driver_id` для хранения только последнего
  известного состояния водителя (см.
  [02-kafka-topics.md](02-kafka-topics.md)).

### PriceCalculated

- **Продюсер**: Pricing Service.
- **Потребители**: Analytics Service. Booking Service может использовать
  событие для read-model/audit, но переход Saga и сумма авторизации
  берутся из результатного события `PriceLocked` из
  [04-saga.md](04-saga.md), а не из публичного `PriceCalculated`.
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
- **Потребители**: Fraud Service (сигнал риска —
  повторные отказы платежа); Analytics Service.
  Booking Service может использовать событие для audit, но компенсация
  Saga запускается по результатному событию `PaymentAuthorizationFailed`
  из [04-saga.md](04-saga.md), а не по публичному `PaymentFailed`.
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

- **Владелец схемы**: Notification Service.
- **Допустимые producers**: доменные сервисы, которым требуется уведомить
  пользователя (Booking, Payments, Payouts, Pricing/Surge — для
  водителей, Driver), публикуют команду по схеме Notification Service. Это
  осознанное исключение для command-события: владелец контракта один, но
  отправителей команды несколько.
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
  поездке (водитель не в поездке тоже шлёт локацию).
- **Ключ партиционирования `geo_cell` для `DriverLocationUpdated`.**
  Отклонено: Kafka log compaction работает по record key. Если ключом
  сделать `geo_cell`, compacted topic сохранит последнее событие на
  геоячейку, а не последнее состояние каждого водителя. Кроме того,
  водитель при движении меняет `geo_cell`, и порядок его обновлений может
  попасть в разные партиции. Локальность для оконных агрегаций достигается
  во Flink через `keyBy(event.payload.geo_cell)`, а не через Kafka record
  key.
- **Публиковать команды шагов саги (`FraudCheckRequested`,
  `PriceLockRequested`, `DriverReservationRequested` и т. п.) как
  самостоятельные публичные доменные события.** Отклонено на уровне этого
  каталога: команды, которыми оркестратор инициирует каждый шаг, — это
  адресные сообщения в Kafka для конкретного участника Saga (см.
  [04-saga.md](04-saga.md)), а не широковещательные факты; в каталог
  включены только публичные факты, которые представляют интерес для
  нескольких потребителей.

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
- Порядок публичных событий жизненного цикла поездки (`BookingCreated` →
  `FraudCheckCompleted` → `PriceCalculated` → `DriverAssigned` →
  `PaymentAuthorized` → `BookingConfirmed` → `RideStarted` →
  `RideCompleted` → `PaymentCaptured` → `PayoutInitiated` →
  `PayoutCompleted`) не должен противоречить внутреннему порядку Saga;
  корректность переходов Saga обеспечивается её результатными событиями
  (`PriceLocked`, `DriverReserved`, `PaymentAuthorized` и отказные пары),
  а не самой Kafka (см. [04-saga.md](04-saga.md)).

## Статус

Предложено.
