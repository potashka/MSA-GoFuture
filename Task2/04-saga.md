# ADR-T2-04: Выбор вида Saga для жизненного цикла бронирования поездки

Дата: 2026-07-17
Авторы: Архитектурный комитет GoFuture, Booking team

## Контекст

Жизненный цикл бронирования поездки — booking → fraud check → pricing →
driver matching → payment authorization — затрагивает пять сервисов
(Booking, Fraud, Pricing, Driver, Payments), каждый из которых владеет
собственными данными (Database-per-Service, см.
[Task1/02-service-map.md](../Task1/02-service-map.md)). Согласованность
между ними не может обеспечиваться распределённой транзакцией (требование
С2, [Task1/01-nfr.md](../Task1/01-nfr.md), явно отклоняет 2PC/XA) — нужен
паттерн Saga. При целевой нагрузке 500 тыс. конкурентных поездок
(требование П1) необходимо явно выбрать вид Saga — оркестрацию или
хореографию — и зафиксировать полную последовательность команд, событий
результата и компенсаций на каждом шаге отказа.

Предыдущая формулировка оставляла неоднозначность: оркестратор мог
синхронно вызвать участника, получить HTTP/gRPC-ответ и одновременно ждать
событие результата из Kafka. Такая модель допускает два независимых сигнала
перехода состояния Saga и усложняет идемпотентность, retry и rollback.

## Требования

- Согласованность денежных потоков через компенсирующие транзакции, а не
  распределённые транзакции (требование С2, [Task1/01-nfr.md](../Task1/01-nfr.md)).
- Идемпотентность операций, инициируемых командами, событиями и ретраями,
  особенно в Payments (требование С3, [Task1/01-nfr.md](../Task1/01-nfr.md)).
- Возможность в любой момент времени однозначно определить, на каком шаге
  находится любая из 500 тыс. одновременных поездок, и почему конкретная
  поездка не продвинулась дальше конкретного шага (диагностируемость,
  согласуется с целевым MTTR < 1 часа, требование Э1,
  [Task1/01-nfr.md](../Task1/01-nfr.md)).
- Компенсация каждого шага отказа должна быть однозначно специфицирована
  заранее, а не выводиться из логов постфактум.
- Латентность создания заказа должна укладываться в бюджет П2
  ([Task1/01-nfr.md](../Task1/01-nfr.md)) на happy path.
- У Saga должен быть один источник перехода состояния: только событие
  результата, полученное оркестратором из Kafka. Синхронные ответы, если
  остаются для отдельных read-only операций, не меняют состояние Saga.

## Решение

**Выбор: асинхронная оркестрируемая Saga**, оркестратор — Booking Service.

Booking Saga Orchestrator хранит состояние процесса в Booking DB:
`saga_id`, `booking_id`, текущее состояние, номер шага, попытки, дедлайны,
последний обработанный `event_id`, историю команд и результатных событий.
Оркестратор отправляет команды участникам через Kafka. Участник
обрабатывает команду идемпотентно и публикует ровно одно логическое событие
результата: успешное или отказное. Только это событие результата переводит
Saga в следующее состояние.

Синхронные HTTP/gRPC-вызовы допускаются только для read-only операций
вроде `GetBookingStatus`, чтения справочных тарифов или диагностических
проверок. Они не дублируют команду, не подтверждают шаг Saga и не меняют
состояние процесса.

### Обоснование выбора оркестрации

- **Сложные компенсации в денежных потоках**: отмена холда платежа и
  возврат водителя в пул — это не независимые локальные реакции каждого
  сервиса на "какое-то предыдущее событие", а часть единого протокола
  отката, зависящего от того, какие именно шаги уже были выполнены к
  моменту отказа. Оркестратор точно знает, какие шаги пройдены: fraud
  пройден, цена зафиксирована, водитель зарезервирован, платёж
  авторизован.
- **Наблюдаемость состояния каждой из 500 тыс. поездок**: у оркестратора
  есть единое место, отвечающее на вопрос "на каком шаге сейчас конкретная
  поездка и почему она не продвинулась дальше" без реконструкции состояния
  по логам пяти независимых сервисов.
- **Отладка в масштабе платформы**: при инциденте, например повышенном
  проценте отказов на шаге Driver Matching, в оркестрации достаточно
  смотреть состояние Booking Saga Orchestrator, его таймауты, команды и
  результатные события. При хореографии логика "что делать дальше"
  размазана по нескольким сервисам.

### Контракт команд и событий Saga

Все команды и события результата Saga используют общий envelope. Для
команд поле `event_id` трактуется как уникальный идентификатор сообщения
команды; для событий — как уникальный идентификатор события.

| Поле | Назначение |
|---|---|
| `event_id` | Уникальный идентификатор сообщения для deduplication. |
| `event_type` | Имя команды или события, например `DriverReservationRequested` или `DriverReserved`. |
| `event_version` | Версия схемы. |
| `saga_id` | Идентификатор экземпляра Saga. |
| `booking_id` | Бизнес-ключ заказа и Kafka record key для Saga-топиков. |
| `correlation_id` | Сквозная корреляция пользовательского запроса и всех сообщений Saga. |
| `causation_id` | `event_id` сообщения, вызвавшего текущую команду или событие. |
| `idempotency_key` | Бизнес-ключ идемпотентности операции участника. |
| `occurred_at` | Время создания сообщения. |
| `region_id` | Регион исполнения Saga. |
| `tenant_id` | Опционально, если включена мультитенантность. |

Пример:

```json
{
  "event_id": "01JZ4W9Q4F2Z9C7R7EV5S5YB4N",
  "event_type": "PaymentAuthorizationRequested",
  "event_version": 1,
  "saga_id": "saga_9d7f",
  "booking_id": "book_123",
  "correlation_id": "corr_456",
  "causation_id": "evt_driver_reserved_789",
  "idempotency_key": "book_123:payment_authorization:v1",
  "occurred_at": "2026-07-17T10:15:30.123Z",
  "region_id": "sea",
  "tenant_id": "gofuture",
  "payload": {
    "amount": 1840,
    "currency": "SGD",
    "payment_method_id": "pm_***"
  }
}
```

### Команды и события результата

| Шаг | Команда оркестратора | Результат участника |
|---|---|---|
| Fraud check | `FraudCheckRequested` | `FraudCheckCompleted` с `verdict=allow/block/review`. |
| Price lock | `PriceLockRequested` | `PriceLocked` или `PriceLockFailed`. |
| Driver reservation | `DriverReservationRequested` | `DriverReserved` или `DriverReservationFailed`. |
| Payment authorization | `PaymentAuthorizationRequested` | `PaymentAuthorized` или `PaymentAuthorizationFailed`. |
| Confirmation | Нет команды участнику | `BookingConfirmed` — терминальный факт, который публикует сам Booking Service. |
| Cancellation | `BookingCancellationRequested` | `BookingCancelled` после фиксации отмены в Booking DB. |
| Driver release | `DriverReleaseRequested` | `DriverReleased` или `DriverReleaseFailed`. |
| Payment release | `PaymentReleaseRequested` | `PaymentReleased` или `PaymentReleaseFailed`. |
| Notification | `NotificationRequested` | Не переводит Saga в новое состояние; это отдельная команда доставки уведомления. |

Участник не публикует одновременно два взаимоисключающих результата одного
шага. Если он обнаружил повтор команды по `idempotency_key`, он повторно
публикует тот же логический результат или отдаёт его из inbox/result store,
не создавая нового бизнес-эффекта.

### State machine

Таймаут не является вторым источником перехода состояния. При истечении
дедлайна оркестратор фиксирует техническое событие результата
`SagaStepTimedOut` через свой outbox и обрабатывает его тем же механизмом,
что и результат участника.

| Состояние | Входное событие | Исходящая команда | Timeout | Следующее состояние | Компенсация |
|---|---|---|---|---|---|
| `NEW` | `BookingCreated` | `FraudCheckRequested` | 2 с | `WAITING_FRAUD` | Нет: внешних резервов ещё нет. |
| `WAITING_FRAUD` | `FraudCheckCompleted(verdict=allow)` | `PriceLockRequested` | 300 мс | `WAITING_PRICE` | Нет. |
| `WAITING_FRAUD` | `FraudCheckCompleted(verdict=block/review)` или `SagaStepTimedOut(step=fraud)` | `BookingCancellationRequested` | 1 с | `CANCELLING` | Нет: внешних резервов ещё нет. |
| `WAITING_PRICE` | `PriceLocked` | `DriverReservationRequested` | 1 с | `WAITING_DRIVER` | При последующей отмене price lock истекает по TTL или снимается локально Pricing Service. |
| `WAITING_PRICE` | `PriceLockFailed` или `SagaStepTimedOut(step=pricing)` | `BookingCancellationRequested` | 1 с | `CANCELLING` | Нет внешнего резерва, кроме локального price lock, если он был частично создан. |
| `WAITING_DRIVER` | `DriverReserved` | `PaymentAuthorizationRequested` | 3 с | `WAITING_PAYMENT` | При последующей отмене отправить `DriverReleaseRequested`. |
| `WAITING_DRIVER` | `DriverReservationFailed` или `SagaStepTimedOut(step=driver)` | `BookingCancellationRequested` | 1 с | `CANCELLING` | Price lock истекает по TTL или снимается Pricing Service. |
| `WAITING_PAYMENT` | `PaymentAuthorized` | Нет команды участнику | 1 с | `COMPLETED` | Нет: Saga успешно завершена, дальнейшие отмены идут отдельным бизнес-сценарием. |
| `WAITING_PAYMENT` | `PaymentAuthorizationFailed` или `SagaStepTimedOut(step=payment)` | `DriverReleaseRequested`, `PaymentReleaseRequested` при неопределённом статусе платежа, `BookingCancellationRequested` | 5 с | `CANCELLING` | Освободить водителя и снять/void платёжный hold, если он мог быть создан. |
| `CONFIRMED` | `BookingCancellationRequested` от клиента или оператора | `DriverReleaseRequested`, `PaymentReleaseRequested`, затем `BookingCancellationRequested` | 5 с | `CANCELLING` | Освободить водителя; снять hold, если списания ещё не было, или запустить refund вне этой Saga после `PaymentCaptured`. |
| `CANCELLING` | `DriverReleased` и/или `PaymentReleased`; затем `BookingCancelled` | `NotificationRequested` | 30 с | `CANCELLED` | Если компенсация не подтверждена, Saga помечается `STUCK_REQUIRES_MANUAL_RESOLUTION`. |

При переходе в `COMPLETED` Booking Service публикует терминальный факт
`BookingConfirmed`, а затем команду `NotificationRequested`. Эти публикации
не являются отдельными входными сигналами перехода Saga.

### Последовательность happy path

1. Клиент отправляет `CreateBooking` через API Gateway в Booking Service.
2. Booking Service в одной локальной транзакции создаёт заказ,
   создаёт запись Saga в состоянии `NEW` и кладёт `BookingCreated` в outbox.
3. Оркестратор потребляет `BookingCreated`, переводит Saga в
   `WAITING_FRAUD` и через outbox публикует `FraudCheckRequested`.
4. Fraud Service потребляет команду, выполняет проверку и публикует
   `FraudCheckCompleted`.
5. Оркестратор потребляет только `FraudCheckCompleted`; при `allow`
   публикует `PriceLockRequested`.
6. Pricing Service фиксирует цену/TTL lock и публикует `PriceLocked`.
7. Оркестратор потребляет `PriceLocked` и публикует
   `DriverReservationRequested`.
8. Driver Service резервирует водителя и публикует `DriverReserved`.
9. Оркестратор потребляет `DriverReserved` и публикует
   `PaymentAuthorizationRequested` с суммой из `PriceLocked`.
10. Payments Service выполняет hold средств и публикует
    `PaymentAuthorized`.
11. Оркестратор потребляет `PaymentAuthorized`, фиксирует `COMPLETED` и
    публикует терминальный факт `BookingConfirmed`.
12. После подтверждения Booking Service публикует `NotificationRequested`
    пассажиру и водителю. Это не часть перехода Saga.

### Надёжность выполнения

- **Transactional outbox**: каждый сервис записывает изменение локального
  состояния и исходящую команду/событие в одной транзакции своей БД.
  Отдельный relay публикует outbox в Kafka. Это не распределённая
  транзакция между сервисами и не 2PC.
- **Inbox и deduplication**: каждый участник хранит обработанные
  `event_id` и `idempotency_key`. Повторная доставка команды становится
  no-op или возвращает прежний результат. Оркестратор аналогично
  дедуплицирует события результата и не применяет одно событие дважды.
- **Retry**: технические ошибки обрабатываются retry с exponential backoff
  и jitter в пределах таймаута шага. Ретраи не должны переживать дедлайн
  шага так, чтобы поздний результат мог создать новый бизнес-эффект после
  компенсации.
- **Timeout**: дедлайн шага хранится в состоянии Saga. Истечение дедлайна
  порождает `SagaStepTimedOut`, а не прямой скрытый переход состояния.
- **DLQ**: после исчерпания retry сообщение отправляется в `{topic}.dlq`
  с причиной, количеством попыток, исходным topic/partition/offset,
  `saga_id`, `booking_id` и `correlation_id`.
- **Ручное разрешение зависших Saga**: если компенсация не подтверждена в
  срок, Saga переводится в `STUCK_REQUIRES_MANUAL_RESOLUTION`. Оператор
  видит историю команд/событий, состояние внешних holds/reservations и
  вручную выбирает продолжить retry, подтвердить компенсацию или
  эскалировать в Payments/Driver.
- **Exactly-once end-to-end не обещается**: Kafka/Flink могут давать
  exactly-once в ограниченном техническом контуре, но сквозной процесс
  через БД сервисов, Kafka, внешние платёжные шлюзы и push-провайдеры
  строится как at-least-once + идемпотентные обработчики + сверка.

## Альтернативы

**Хореография**: каждый сервис реагирует на событие предыдущего шага
самостоятельно, без центрального координатора; компенсации реализуются как
отдельные подписки на события отказа. Отклонено: нет единого места, которое
знает полный пройденный путь конкретного заказа, а ответственность за
компенсацию денежных операций и возврат водителя размывается между
сервисами.

**Гибридная Saga с синхронными командами и асинхронными событиями
результата**: оркестратор вызывает участника по HTTP/gRPC и дополнительно
ждёт событие результата. Отклонено как базовая модель: появляется два
сигнала перехода состояния, возникают гонки между ответом и событием,
усложняются retry и rollback. HTTP/gRPC оставляется только для read-only
операций и не меняет состояние Saga.

**Распределённая транзакция 2PC/XA между Booking, Driver и Payments**:
отклонена требованиями С2 и эксплуатационно неприемлема для Tier-1 пути
создания заказа.

## Компромиссы и риски

- Booking Service как оркестратор становится критичным компонентом для
  процесса бронирования. Требует высокой доступности, репликации состояния
  и восстановления timers/outbox после рестарта.
- Асинхронная модель увеличивает end-to-end latency happy path по сравнению
  с локальным синхронным вызовом. Это компенсируется независимым
  масштабированием участников, backpressure через Kafka и отсутствием
  каскадного ожидания сетевых вызовов.
- Компенсации могут быть выполнены позже исходного отказа. Payments и
  Driver должны поддерживать идемпотентные release/void операции и
  явно различать "не было резерва", "резерв снят" и "резерв требует
  ручной проверки".
- Команда `NotificationRequested` имеет нескольких допустимых producers
  по единой схеме Notification Service (см.
  [01-domain-events.md](01-domain-events.md)); она не участвует в
  переходах состояния Saga.

## Статус

Предложено.
