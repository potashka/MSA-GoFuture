# Task1 — Декомпозиция монолита GoFuture на доменные сервисы

Артефакты плана поэтапной декомпозиции монолита GoFuture на сервисы по
продуктовым доменам. Стратегия — Strangler Fig + Database-per-Service,
миграция с минимизацией простоя. Исходный контекст — в
[docs/context.md](../docs/context.md), шаблон ADR — в
[docs/adr-template.md](../docs/adr-template.md).

## Содержание

| Файл | Описание |
|---|---|
| [01-nfr.md](01-nfr.md) | Нефункциональные требования к целевой архитектуре (ADR): производительность, доступность, масштабируемость, безопасность, эксплуатируемость, согласованность данных — каждое требование измеримо и с целевым значением. |
| [02-service-map.md](02-service-map.md) | Карта целевых сервисов (ADR): доменные bounded context (Booking, Driver, Pricing, Payments, Payouts, Notification, Geography, Fraud, Analytics) и платформенные сервисы (API Gateway, Identity Service) — владеющая команда, собственные данные, публикуемые/потребляемые события, синхронные API. |
| [03-decomposition-order.md](03-decomposition-order.md) | ADR об очерёдности выделения сервисов из монолита: 7 этапов от Notification (пилот) до Booking (последним), с обоснованием по логике "минимальная связанность → максимальная бизнес-ценность → нарастающий риск", годовым планом декомпозиции и критериями готовности перехода между этапами. |
| [c2-to-be.puml](c2-to-be.puml) | Диаграмма контейнеров C2 To-Be (C4-PlantUML): клиенты → API Gateway → доменные сервисы со своими БД, Kafka как событийная шина, Identity Service, "усыхающий" монолит за тем же Gateway, Anti-Corruption Layer, CDC (Debezium) из БД монолита.  |
| [04-backward-compatibility.md](04-backward-compatibility.md) | ADR о механизмах обратной совместимости на время миграции: постепенное переключение маршрутов в API Gateway, версионирование API (заморозка v1), feature flags с мгновенным откатом, Anti-Corruption Layer, contract-тесты. |
| [05-data-migration-plan.md](05-data-migration-plan.md) | План миграции данных (ADR): общий пайплайн snapshot/backfill → односторонний CDC → shadow reads/reconciliation → controlled writer cutover → вывод старых таблиц, детализированный по каждому из 9 доменных сервисов (таблицы, стратегия, план отката, критерий успеха сверки). |

## Как читать

Документы образуют единую цепочку решений: [01-nfr.md](01-nfr.md) задаёт
измеримые цели → [02-service-map.md](02-service-map.md) определяет границы
сервисов, удовлетворяющих этим целям → [03-decomposition-order.md](03-decomposition-order.md)
задаёт порядок и годовой план их выделения → [c2-to-be.puml](c2-to-be.puml) визуализирует
целевое состояние переходного периода → [04-backward-compatibility.md](04-backward-compatibility.md)
и [05-data-migration-plan.md](05-data-migration-plan.md) описывают механику
самого перехода на каждом этапе.

## Соответствие диаграммам AS-IS

Диаграммы AS-IS (C1/C2/C3 в формате C4-PlantUML) в `docs/as-is/`

из AS-IS диаграмм" в [docs/context.md](../docs/context.md). Очерёдность
декомпозиции в [03-decomposition-order.md](03-decomposition-order.md)
: диаграмма компонентов
(C3) показывает, что Booking Domain синхронно вызывает шесть других
доменов — больше, чем любой другой домен, что количественно подтверждает
решение выносить Booking последним; Notification и Payouts уже
взаимодействуют с внешними системами асинхронно через Celery-задачи, что
подтверждает их пригодность для ранних этапов; Fraud действительно
вызывается синхронно из Booking, как и предполагалось в
[02-service-map.md](02-service-map.md); Analytics уже сегодня и читает
напрямую все таблицы по SQL, и параллельно получает события — подтверждает
как её крайнюю связанность в AS-IS, так и то, что частичная событийная
инфраструктура для неё уже существует.

## Скриншоты диаграмм

Готовый PNG-рендер [c2-to-be.puml](c2-to-be.puml) — в
[../renders/Task1/](../renders/Task1/).
