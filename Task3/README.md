# Task3 — Глобальное развёртывание GoFuture

Артефакты дизайна глобального развёртывания для 99,99% доступности
Tier-1 ride APIs в целевых production-регионах и низкой задержки в
Юго-Восточной Азии и Южной Америке. Развивает Task1 (карта сервисов,
C2 To-Be) и Task2 (событийная платформа, региональная модель Kafka-топиков).
Исходный контекст — [docs/context.md](../docs/context.md), шаблон ADR —
[docs/adr-template.md](../docs/adr-template.md).

## Содержание

| Файл | Описание |
|---|---|
| [01-region-selection.md](01-region-selection.md) | ADR о выборе 4 регионов: Сингапур (`sgp`, хаб ЮВА), Джакарта (`jkt`, крупнейший рынок ЮВА + latency/risk/residency policy), Сан-Паулу (`sao`, Бразилия + latency/risk/residency policy), домашний регион (`core`, админка и глобальные сервисы). Сравнение с альтернативами (Куала-Лумпур, Богота). |
| [02-replication.md](02-replication.md) + [replication-schema.puml](replication-schema.puml) | ADR и диаграмма схемы репликации: региональный шардинг для данных поездок/локаций/платежей, асинхронная глобальная репликация справочников/конфигурации/тонкого IAM-индекса, synchronous/semisynchronous PostgreSQL standby с fencing, Kafka RF>=3/min.insync.replicas/acks=all, Redis только как cache, MirrorMaker 2 или аналог только для разрешённых DR-потоков. |
| [03-geo-routing.md](03-geo-routing.md) + [geo-routing-schema.puml](geo-routing-schema.puml) | ADR и диаграмма механизма геомаршрутизации: GeoDNS + Anycast определяют `current_edge_region`, `profile_region` хранит профиль/IAM/account data, а `ride_region` выбирается по pickup/city/tenant/market и закрепляет операционную поездку за регионом оказания услуги. |
| [04-failover.md](04-failover.md) + [failover-c4.puml](failover-c4.puml) | ADR и C4-диаграмма аварийного переключения: Tier-1 ride APIs, multi-AZ baseline, точки отказа (pod/node/AZ/регион, Kafka, PostgreSQL, Redis, внешние провайдеры, межрегиональная связь), численные RTO/RPO, split-brain controls, regional DR и degraded mode при residency-ограничениях. |
| [05-compliance.md](05-compliance.md) | ADR архитектурной стратегии комплаенса: data classification, residency matrix, data localization, cross-border transfer через legal gate, retention, deletion/DSAR, audit, break-glass, incident notification и PCI scope через токенизацию. |
| [c2-to-be-security.puml](c2-to-be-security.puml) | Доработанная диаграмма C2 из [Task1/c2-to-be.puml](../Task1/c2-to-be.puml) с инструментами защиты, routing policy и HA baseline: GSLB/GeoDNS/Anycast, WAF, DDoS-защита, Network Security Groups/Firewall, Service Mesh (mTLS), Secrets Manager, Ride Region Resolver, Profile Attributes Projection, Regional HA Baseline и DR Control/Fencing. Новые/изменённые элементы выделены красным (`UpdateElementStyle`), легенда включена. |
| [06-security-rationale.md](06-security-rationale.md) | Текстовое обоснование механизмов защиты и безопасной геомаршрутизации со ссылкой на диаграмму. |

## Как читать

[01-region-selection.md](01-region-selection.md) фиксирует, *где* работает
платформа → [02-replication.md](02-replication.md) фиксирует, *где живут
данные* и как они синхронизируются → [03-geo-routing.md](03-geo-routing.md)
фиксирует, как запрос пользователя попадает в ближайший edge, а поездка
получает `ride_region` независимо от `profile_region` →
[04-failover.md](04-failover.md) фиксирует, что происходит при отказе на
любом из этих уровней → [05-compliance.md](05-compliance.md) фиксирует
регуляторный процессный слой поверх регионального дизайна →
[c2-to-be-security.puml](c2-to-be-security.puml) и
[06-security-rationale.md](06-security-rationale.md) добавляют периметровую
и внутреннюю защиту поверх всей архитектуры.

## Согласованные уточнения к предыдущим задачам

- Условные обозначения регионов `sea`/`sam` из
  [Task2/02-kafka-topics.md](../Task2/02-kafka-topics.md) конкретизированы
  в [01-region-selection.md](01-region-selection.md) до `sgp`+`jkt`
  (Юго-Восточная Азия — два региона по разным причинам) и `sao` (Южная
  Америка).
- Целевая доступность 99,99% для Tier-1 ride APIs
  ([01-region-selection.md](01-region-selection.md),
  [04-failover.md](04-failover.md)) является усиленной целью Task3 для
  целевых production-регионов. Требование Д1 в
  [Task1/01-nfr.md](../Task1/01-nfr.md) (99,95%) остаётся бизнес-NFR для
  общей платформы; показатели Tier 2/3 не пересматриваются.
- "Профили", упомянутые как глобально реплицируемые в задаче, уточнены в
  [02-replication.md](02-replication.md) до тонкого индекса (глобальный ID
  + `profile_region`) — содержимое профиля с персональными данными
  по умолчанию остаётся региональным согласно архитектурной
  residency/cross-border policy из [05-compliance.md](05-compliance.md).

## Проверка диаграмм

Все 4 файла `.puml` в этой директории скомпилированы локально через
PlantUML (`plantuml.jar`, с реальной загрузкой стандартной библиотеки
C4-PlantUML из интернета) без ошибок компиляции.

## Скриншоты диаграмм

Готовые PNG-рендеры всех 4 диаграмм ([replication-schema.puml](replication-schema.puml),
[geo-routing-schema.puml](geo-routing-schema.puml),
[failover-c4.puml](failover-c4.puml),
[c2-to-be-security.puml](c2-to-be-security.puml)) — в
[../renders/Task3/](../renders/Task3/).
