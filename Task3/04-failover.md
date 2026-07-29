# ADR-T3-04: Аварийное переключение (failover)

Дата: 2026-07-17
Авторы: Архитектурный комитет GoFuture, SRE

## Контекст

В [Task1/01-nfr.md](../Task1/01-nfr.md) зафиксировано бизнес-требование
99,95% доступности критических сервисов общей платформы. Task3 вводит
усиленную цель: **99,99% для Tier-1 ride APIs в целевых регионах** при
мультизональном развёртывании, региональном шардинге данных
([02-replication.md](02-replication.md)) и геомаршрутизации
([03-geo-routing.md](03-geo-routing.md)).

Эти цели не равны:

- 99,95% — базовая доступность критических сервисов платформы как
  бизнес-NFR Task1;
- 99,99% — целевая планка Task3 для пользовательского hot path поездки в
  production-регионах, где уже выполнены multi-AZ, автоматический failover
  внутри региона и подготовленный DR-сценарий.

Полный отказ региона не должен по умолчанию означать "несколько часов"
простоя для Tier-1. Но архитектура также не обещает межгосударственный
failover персональных или регуляторно значимых данных до юридической
квалификации, подтверждения местного юриста и настройки допустимого
механизма передачи ([05-compliance.md](05-compliance.md)).

## Требования

- Tier-1 ride APIs включают:
  - создание поездки;
  - обновление статуса поездки;
  - поиск/резервирование водителя;
  - критическую часть Pricing, необходимую для подтверждения заказа;
  - авторизацию платежа.
- Каждый production-регион (`sgp`, `jkt`, `sao`) должен иметь минимум:
  несколько зон доступности, stateless Kubernetes workloads в нескольких
  AZ, PodDisruptionBudget, anti-affinity/topology spread, autoscaling и
  несколько ingress/API Gateway instances.
- Каждый сценарий отказа должен иметь обнаружение, механизм failover,
  численные RTO/RPO, режим деградации и условие возврата.
- PostgreSQL использует synchronous/semisynchronous replication внутри
  региона, fencing старого primary и защиту от split-brain.
- Kafka использует replication factor не менее 3 внутри production-региона,
  `min.insync.replicas`, producer `acks=all`; межрегиональные потоки через
  MirrorMaker 2 или аналог разрешены только для данных, допустимых
  residency policy.
- Redis не является источником истины: при отказе допускается потеря кэша
  и восстановление из PostgreSQL/Kafka/read models.
- Geo-routing должен использовать health checks, GSLB/DNS, низкий TTL,
  connection draining, исключение неисправного региона и защиту от
  flapping.

## Решение

### Tier-1 HA baseline внутри каждого региона

Для `sgp`, `jkt`, `sao` принимается одинаковый production baseline:

- Kubernetes node pools распределены минимум по 3 AZ.
- Stateless workloads Tier-1 имеют минимум 3 реплики на сервис, PDB
  `minAvailable` не ниже 2 для обычного maintenance.
- `podAntiAffinity` и `topologySpreadConstraints` не допускают размещения
  всех реплик одного сервиса в одной AZ или на одном node pool.
- HPA/KEDA масштабируют Booking, Driver, Pricing, Payments Adapter,
  Gateway и Saga consumers по RPS, CPU, latency, Kafka lag и длине outbox.
- API Gateway/Ingress развёрнут несколькими инстансами в разных AZ;
  unhealthy instances исключаются из локального load balancer.
- Redis/кэши развёрнуты multi-AZ, но считаются восстановимыми; source of
  truth остаётся в PostgreSQL, Kafka и доменных БД.

### Таблица сценариев отказа

| Сценарий | Обнаружение | Failover | RTO | RPO | Деградация | Условие возврата |
|---|---|---|---|---|---|---|
| Отказ pod Tier-1 сервиса | Kubernetes liveness/readiness/startup probes, RED-метрики | Автоматический restart/reschedule; Service исключает pod из endpoints | ≤ 1 мин | 0 для stateless pod; состояние во внешних хранилищах | Обычно незаметно; краткий рост latency | Новый pod healthy, readiness проходит, error rate вернулся к baseline |
| Отказ node | Node heartbeat, kubelet/node-problem-detector, cloud health | Автоматический reschedule pod на другой node/AZ; PDB ограничивает добровольные disruptions | ≤ 2 мин | 0 для stateless workloads | Снижение запаса мощности до reschedule/autoscaling | Node заменён или исключён, реплики снова распределены по topology spread |
| Отказ Availability Zone | LB/AZ health checks, cloud status, Prometheus/Alertmanager | Автоматически: Gateway исключает AZ, Kubernetes reschedule в здоровые AZ, PostgreSQL/Kafka используют surviving replicas | ≤ 5 мин | PostgreSQL: 0 или ≤ 1 мин в зависимости от sync/semisync; Kafka: подтверждённые записи сохраняются при `acks=all` и ISR | Работа в 2 AZ с меньшим capacity; autoscaling поднимает запас | AZ healthy, данные догнаны, трафик возвращается через connection draining |
| Отказ Kafka broker | Kafka controller, under-replicated partitions, ISR shrink, broker health | Автоматический leader election среди ISR; broker replacement | ≤ 1 мин | Для подтверждённых сообщений RPO 0 при RF≥3, `min.insync.replicas≥2`, `acks=all`; неподтверждённые produce-запросы повторяются идемпотентно | Временное снижение throughput, возможен рост consumer lag | ISR восстановлен, under-replicated partitions = 0, lag снижается |
| Отказ PostgreSQL primary | Managed DB health, connection errors, replication lag, Patroni/operator | Автоматический promotion sync/semisync standby; fencing старого primary перед открытием записи | ≤ 5–10 мин | 0 при synchronous commit; ≤ 1 мин при semisynchronous/asynchronous tail внутри региона | Краткая недоступность записи; read-only для части операций | Старый primary fenced/reimaged, новый primary единственный writer, lag replicas = 0 |
| Отказ Redis | Redis health, cache error rate, latency, memory/eviction alerts | Автоматический failover replica или пересоздание cache cluster | ≤ 2 мин | Допускается потеря кэша; source of truth не в Redis | Рост latency, fallback на PostgreSQL/Kafka/read model; throttling при необходимости | Cache warmed, latency и hit ratio восстановлены |
| Отказ внешнего платёжного провайдера | Circuit breaker, timeout/error rate, provider status webhook | Автоматически: переключение на резервный локальный провайдер, если разрешён; иначе fast-fail `PaymentAuthorizationFailed` и Saga compensation | ≤ 1 мин для переключения/fast-fail | Н/п: не начатые авторизации не считаются потерянными; начатые сверяются по idempotency key/provider reconciliation | Новые заказы могут не подтверждаться или предлагается другой способ оплаты | Provider healthy, reconciliation завершён, circuit breaker half-open → closed |
| Отказ картографического сервиса | Circuit breaker, latency/error rate, synthetic checks | Автоматически: резервный provider или degraded route/ETA cache | ≤ 1 мин | Н/п | Менее точные ETA/маршруты; при невозможности pickup validation `CreateBooking` отклоняется безопасно | Provider healthy, cache актуализирован, circuit breaker closed |
| Полный отказ региона: разрешённый DR внутри той же юридической зоны | GSLB/Anycast health checks по всем AZ, cloud status, SRE confirmation | Частично автоматизировано: регион исключается из GSLB/Anycast, DR runbook поднимает warm standby в той же юридической зоне, PostgreSQL replica promoted с fencing, Kafka разрешённые топики восстановлены/переключены | ≤ 15–30 мин | PostgreSQL: по async DR lag, целевое RPO ≤ 5 мин; Kafka: RPO по MirrorMaker 2 lag для разрешённых потоков, целевое ≤ 5 мин | Краткая недоступность Tier-1 ride APIs этого market; часть активных Saga требует reconciliation | Старый регион fenced, DR-регион объявлен writer, DNS/GSLB стабилен, reconciliation и game day checks пройдены |
| Полный отказ региона: cross-border DR не квалифицирован или не разрешён policy | Те же сигналы + compliance policy не разрешает перенос данных без legal qualification | Автоматически исключается неисправный регион; операционный degraded mode без создания поездок в этом `ride_region` до восстановления или локальной DR-площадки | Edge failover ≤ 5 мин; восстановление ride APIs зависит от локальной площадки: целевой RTO ≤ 30 мин при наличии локального standby, иначе сервис недоступен до восстановления региона | Межрегиональный RPO не применяется: данные не реплицируются за пределы юридической зоны | Новые поездки в market недоступны; профильные операции в других `profile_region` продолжают работать; пользователям показывается региональная деградация | Основной регион восстановлен или активирована разрешённая локальная DR-площадка; legal qualification подтверждена |
| Потеря связи между регионами | Inter-region synthetic checks, MirrorMaker lag, replication lag, GSLB telemetry | Автоматически: регионы продолжают локально обслуживать `ride_region`; останавливаются/буферизуются только разрешённые cross-region async flows | Для локальных Tier-1 APIs: 0–1 мин; для глобальных витрин: до восстановления связи | Локальные данные: 0; async global exports/MirrorMaker: RPO равен накопленному lag, целевой ≤ 15 мин для разрешённых данных | Глобальная аналитика/IAM projection может устареть; новые profile changes могут распространяться позже | Межрегиональная связь стабильна, lag догнан, отложенные события сверены |

### Полный отказ региона: два режима

**1. DR в резервную площадку внутри той же юридической зоны.** Для markets,
где принята policy локальной обработки, целевой дизайн — warm standby в
той же стране/юридической зоне. Он содержит заранее созданную
инфраструктуру, пустые или реплицируемые разрешённым способом БД, Kafka
кластер и Gateway. Promotion выполняется по runbook с fencing старого
primary и защитой от split-brain. Цель для Tier-1 ride APIs: RTO ≤ 15–30
мин, RPO по фактическому lag репликации, целевое ≤ 5 мин.

**2. Ограниченный degraded mode, если перенос данных не квалифицирован.**
Если для региона нет разрешённой локальной DR-площадки, а cross-border
обработка персональных/платёжных/операционных данных не прошла legal
qualification, система не переносит эти данные в другой регион.
GSLB/Anycast исключает неисправный регион, но `CreateBooking` для
затронутого `ride_region` возвращает управляемую ошибку/экран деградации.
Остальные регионы продолжают работать.

### Split-brain protection (Защита от одновременной работы двух primary-узлов)

- PostgreSQL promotion разрешён только после гарантированной изоляции старого primary:
  cloud API detach/stop, lease/consensus lock, STONITH или managed DB
  гарантия единственного writer.
- Сервисы получают новый writer endpoint только после смены leader lease.
- Старый primary после восстановления не принимает запись, пока не пройдёт
  rejoin/rebase как replica или полная пересборка.
- Для Kafka запрещён unclean leader election для Tier-1 топиков; запись
  идёт с `acks=all`, `min.insync.replicas≥2`.
- DR cutover и failback требуют reconciliation outbox/Kafka offsets и
  idempotency keys Saga/Payments.

### Geo-routing failover

GSLB/DNS и Anycast используют health checks по регионам и AZ, TTL 30–60
секунд для DNS-записей Tier-1 endpoints, connection draining перед
исключением endpoint из ротации и cooldown/hold-down окна против flapping.
Регион возвращается в ротацию только после прохождения synthetic checks,
восстановления capacity, Kafka ISR, PostgreSQL replication health и
подтверждения SRE для региональных инцидентов.

Диаграмма: [failover-c4.puml](failover-c4.puml).

## Альтернативы

- **Полностью автоматический cross-border failover региона без участия
  человека.** Отклонено: может нарушить принятую residency/cross-border
  policy, если передача не квалифицирована местного юриста, и создать
  split-brain при ложном срабатывании. Автоматика исключает неисправный
  регион из routing, но promotion DR writer требует runbook, fencing и
  проверки policy.
- **Один RTO "часы" для полного отказа региона.** Отклонено для Tier-1
  ride APIs: такая цель несовместима с 99,99%. Вместо этого вводится
  warm/local DR в той же юридической зоне с RTO ≤ 15–30 мин там, где
  перенос данных разрешён; где cross-border transfer не квалифицирован
  или не разрешён policy — явно описан degraded mode.
- **Redis как источник истины для доступности/локации водителя.**
  Отклонено: потеря Redis должна приводить к rebuild cache, а не к потере
  бизнес-состояния. Источник истины — доменная БД/события Kafka.

## Компромиссы и риски

- 99,99% для Tier-1 ride APIs требует постоянной стоимости: лишние
  реплики, capacity buffer, несколько AZ, warm standby и регулярные game
  days. Для общей платформы остаётся базовая цель 99,95% из Task1.
- DR в той же юридической зоне может быть недоступен у выбранного
  провайдера в конкретной стране. Тогда нужно либо выбрать второго
  провайдера в той же юрисдикции, либо честно принять degraded mode для
  полного регионального disaster.
- Асинхронная межрегиональная репликация разрешённых данных имеет RPO,
  равный фактическому lag; она не используется как механизм отсутствия
  потери данных для персональных/платёжных данных.
- Failback после регионального disaster сложнее failover: требуется
  сверка writers, Kafka offsets, outbox/inbox, payment reconciliation и
  контролируемое возвращение GSLB-трафика.

## Статус

Предложено.
