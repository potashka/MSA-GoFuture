# Task5 — Мультитенантная платформа GoFuture

Артефакты дизайна мультитенантной платформы для быстрого запуска
партнёров в новых регионах с полной изоляцией данных и кастомизацией.
Развивает Database-per-Service и карту сервисов Task1, каталог событий и
Kafka-модель Task2, региональную модель и IAM/security-механизмы Task3,
платформу данных Task4. Исходный контекст —
[docs/context.md](../docs/context.md), шаблон ADR —
[docs/adr-template.md](../docs/adr-template.md).

## Содержание

| Файл | Описание |
|---|---|
| [01-tenancy-model.md](01-tenancy-model.md) | ADR модели изоляции данных: гибрид pool (общие таблицы + PostgreSQL RLS по `tenant_id` + выделенные партиции Kafka) для стандартных партнёров и silo (отдельная схема/БД, при жёсткой регуляторике — отдельный региональный стек) для крупных; критерии выбора уровня, сравнение с чистыми pool/silo. |
| [02-iam.md](02-iam.md) | ADR IAM-системы: Keycloak, realm на тенанта, SSO через OIDC/SAML с корпоративными IdP партнёров, RBAC. Таблица из 9 ролей (роль/область/доступ к данным/ключевые операции). |
| [03-onboarding.md](03-onboarding.md) | ADR автоматизированного онбординга: 9 шагов от заявки через API самообслуживания до активации, с указанием автоматизации (Terraform для одноразового провижининга, Kubernetes operator для постоянного reconciliation), целевое время 2–4 недели (pool/silo-схема) — с явной оговоркой, что silo "выделенный региональный стек" в этот срок не укладывается. |
| [c2-multitenancy.puml](c2-multitenancy.puml) | Диаграмма контейнеров (C4): IAM (Keycloak), мониторинг с меткой `tenant_id` (Prometheus/Grafana/Alertmanager/OTel), квоты и rate limiting на API Gateway, pool/silo БД, Kafka с tenant-изоляцией. Проверена локальной компиляцией PlantUML без ошибок. |
| [c3-onboarding.puml](c3-onboarding.puml) | Диаграмма компонентов (C4): внутреннее устройство Onboarding Service (Self-Service API, оркестратор, Terraform Runner, Tenant Operator, Smoke Test Runner) и потоки к Keycloak/БД/Kafka/Gateway/Grafana/Secrets Manager/внешним провайдерам. Проверена локальной компиляцией PlantUML без ошибок. |

## Как читать

[01-tenancy-model.md](01-tenancy-model.md) определяет, *что именно*
изолируется между тенантами и как → [02-iam.md](02-iam.md) определяет,
*кто* и с каким уровнем доступа действует в этой модели →
[03-onboarding.md](03-onboarding.md) определяет, *как* всё это
автоматически создаётся для нового партнёра. Диаграммы — визуальная сводка
целевой архитектуры ([c2-multitenancy.puml](c2-multitenancy.puml)) и
самого процесса онбординга ([c3-onboarding.puml](c3-onboarding.puml)).

## Согласованность с предыдущими задачами

- `tenant_id` добавлен как обязательное поле конверта каждого доменного
  события ([Task2/01-domain-events.md](../Task2/01-domain-events.md)),
  наравне с уже существующими полями схемы — уточнение каталога событий,
  а не его замена.
- Онбординг переиспользует паттерн оркестрации из
  [Task2/04-saga.md](../Task2/04-saga.md) (Onboarding Service как
  оркестратор процесса) и практику обязательных проверок перед
  завершением этапа из
  [Task1/03-decomposition-order.md](../Task1/03-decomposition-order.md) и
  [Task1/04-backward-compatibility.md](../Task1/04-backward-compatibility.md).
- Silo-уровень "выделенный региональный стек"
  ([01-tenancy-model.md](01-tenancy-model.md)) явно сведён к тому же
  процессу, что добавление нового региона
  ([Task3/01-region-selection.md](../Task3/01-region-selection.md)), а не
  описан заново — с соответствующим более долгим сроком, вне целевых
  2–4 недель типового онбординга.
