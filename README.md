# MSA-GoFuture

Архитектурная документация миграции такси-агрегатора GoFuture с монолита
(Django + Celery + RabbitMQ, единая PostgreSQL) на целевую микросервисную
платформу. Весь материал оформлен как последовательность ADR (Architecture
Decision Records, см. [docs/adr-template.md](docs/adr-template.md)) поверх
общего контекста в [docs/context.md](docs/context.md).

## Суть решения

Декомпозиция монолита ведётся по стратегии **Strangler Fig** с
**Database-per-Service**, без big bang: сервисы выделяются в порядке
нарастающего риска (от почти stateless Notification до Booking —
оркестратора, вокруг которого исторически завязан весь монолит), с
API Gateway/ACL/feature-флагами для обратной совместимости и явным планом
миграции данных (CDC → dual-write → сверка → переключение) на каждом шаге
(Task1). Поверх выделенных доменов построена **событийная платформа на
Kafka**: каталог доменных событий со Schema Registry, Flink/Kafka Streams
для обработки в реальном времени (динамическое ценообразование, подбор
водителей) и **оркестрируемая Saga** в Booking Service для жизненного
цикла поездки — оркестрация выбрана вместо хореографии ради
наблюдаемости состояния каждой из 500 тыс. конкурентных поездок и
управляемых компенсаций в денежных потоках (Task2).

Платформа развёрнута в **4 регионах** (Сингапур, Джакарта, Сан-Паулу,
домашний регион) с **региональным шардингом данных** вместо
глобальной active-active репликации — latency и резидентность данных
(UU PDP, LGPD) решаются одним и тем же архитектурным приёмом; глобально
реплицируются только справочники, конфигурация и тонкий IAM-индекс.
Геомаршрутизация (GeoDNS + Anycast + region affinity в токене), явные
границы автоматизации аварийного переключения и защищённый периметр
(WAF, DDoS-защита, Service Mesh с mTLS, Secrets Manager) закрывают
требования доступности и безопасности глобального масштаба (Task3).

Единая **платформа данных и ML** строится на тех же событиях и CDC:
региональная обработка Flink пишет в ClickHouse (BI) и Data Lake (S3),
анонимизированный/агрегированный экспорт — единственный легальный канал
в центральный аналитический регион; Feature Store и ML-платформа
(training, model registry, serving) отдают предсказания обратно в Pricing
и Fraud с контролем качества данных на каждом этапе пайплайна (Task4).
Наконец, **мультитенантность** позволяет быстро запускать партнёров в
новых регионах: гибридная изоляция pool (PostgreSQL RLS + выделенные
партиции Kafka) и silo (отдельная схема/БД или целый выделенный
региональный стек для жёсткой регуляторики), Keycloak с realm на тенанта
и SSO через корпоративные IdP партнёров, полностью автоматизированный
онбординг (Terraform + Kubernetes operator) с целью 2–4 недели на
типового партнёра (Task5).

## Задание | Артефакты | Ссылки

| Задание | Артефакты | Ссылки |
|---|---|---|
| **Контекст и шаблон** — бизнес-требования, AS-IS, стек, оргструктура; шаблон ADR | 2 документа | [docs/context.md](docs/context.md)<br>[docs/adr-template.md](docs/adr-template.md) |
| **Task1** — декомпозиция монолита (Strangler Fig + Database-per-Service) | НФТ, карта сервисов, очерёдность декомпозиции, обратная совместимость, план миграции данных, диаграмма C2 To-Be, оглавление | [Task1/01-nfr.md](Task1/01-nfr.md)<br>[Task1/02-service-map.md](Task1/02-service-map.md)<br>[Task1/03-decomposition-order.md](Task1/03-decomposition-order.md)<br>[Task1/04-backward-compatibility.md](Task1/04-backward-compatibility.md)<br>[Task1/05-data-migration-plan.md](Task1/05-data-migration-plan.md)<br>[Task1/c2-to-be.puml](Task1/c2-to-be.puml)<br>[Task1/README.md](Task1/README.md) |
| **Task2** — событийная платформа (Kafka, Saga, надёжность, мониторинг) | Каталог событий, схема Kafka-топиков, потоковая обработка, ADR Saga, гарантии доставки, мониторинг, диаграмма C2, оглавление | [Task2/01-domain-events.md](Task2/01-domain-events.md)<br>[Task2/02-kafka-topics.md](Task2/02-kafka-topics.md)<br>[Task2/03-stream-processing.md](Task2/03-stream-processing.md)<br>[Task2/04-saga.md](Task2/04-saga.md)<br>[Task2/05-delivery-guarantees.md](Task2/05-delivery-guarantees.md)<br>[Task2/06-monitoring.md](Task2/06-monitoring.md)<br>[Task2/c2-event-platform.puml](Task2/c2-event-platform.puml)<br>[Task2/README.md](Task2/README.md) |
| **Task3** — глобальное развёртывание (регионы, репликация, гео-роутинг, failover, комплаенс, безопасность) | Выбор регионов, репликация + схема, гео-роутинг + схема, failover + C4-диаграмма, комплаенс, защищённая диаграмма C2, обоснование security, оглавление | [Task3/01-region-selection.md](Task3/01-region-selection.md)<br>[Task3/02-replication.md](Task3/02-replication.md) · [Task3/replication-schema.puml](Task3/replication-schema.puml)<br>[Task3/03-geo-routing.md](Task3/03-geo-routing.md) · [Task3/geo-routing-schema.puml](Task3/geo-routing-schema.puml)<br>[Task3/04-failover.md](Task3/04-failover.md) · [Task3/failover-c4.puml](Task3/failover-c4.puml)<br>[Task3/05-compliance.md](Task3/05-compliance.md)<br>[Task3/c2-to-be-security.puml](Task3/c2-to-be-security.puml)<br>[Task3/06-security-rationale.md](Task3/06-security-rationale.md)<br>[Task3/README.md](Task3/README.md) |
| **Task4** — платформа данных и ML | Архитектура пайплайна, диаграмма C4, качество данных, оглавление | [Task4/01-data-pipeline.md](Task4/01-data-pipeline.md)<br>[Task4/c4-data-pipeline.puml](Task4/c4-data-pipeline.puml)<br>[Task4/02-data-quality.md](Task4/02-data-quality.md)<br>[Task4/README.md](Task4/README.md) |
| **Task5** — мультитенантность и онбординг партнёров | Модель изоляции, IAM/RBAC, онбординг, диаграммы C2/C3, оглавление | [Task5/01-tenancy-model.md](Task5/01-tenancy-model.md)<br>[Task5/02-iam.md](Task5/02-iam.md)<br>[Task5/03-onboarding.md](Task5/03-onboarding.md)<br>[Task5/c2-multitenancy.puml](Task5/c2-multitenancy.puml)<br>[Task5/c3-onboarding.puml](Task5/c3-onboarding.puml)<br>[Task5/README.md](Task5/README.md) |

Все ссылки в таблице проверены (файлы существуют по указанным путям), все
`.puml`-диаграммы скомпилированы локально через PlantUML без ошибок, в
каждой директории `Task1`–`Task5` есть собственный `README.md` с более
подробным оглавлением и порядком чтения документов внутри задания.

## Скриншоты диаграмм

В [renders/](renders/) лежат заранее отрисованные PNG-скриншоты всех
`.puml`-диаграмм — по одной подпапке на задание (`renders/Task1/` …
`renders/Task5/`), внутри файл с тем же именем, что и у исходной
диаграммы в `Task1`–`Task5`. Нужны для быстрого просмотра без локальной
компиляции PlantUML.
