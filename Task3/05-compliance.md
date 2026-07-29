# ADR-T3-05: Архитектурная стратегия комплаенса данных

Дата: 2026-07-17
Авторы: Архитектурный комитет GoFuture, Payments & Payouts team, юридическая функция (консультативно)

## Контекст

Выход на рынки Индонезии (`jkt`) и Бразилии (`sao`, см.
[01-region-selection.md](01-region-selection.md)) добавляет локальные
регуляторные режимы, включая UU PDP и LGPD, поверх общего требования
безопасности и data residency из Task1 (требование Б3,
[Task1/01-nfr.md](../Task1/01-nfr.md); [docs/context.md](../docs/context.md)).

Этот ADR не является юридическим заключением и не утверждает, что
конкретный закон всегда абсолютно запрещает любую трансграничную передачу
данных. В рамках архитектуры принимается более консервативное
**архитектурное допущение**: персональные, платёжные и точные
операционные данные по умолчанию остаются в регионе обработки, а
cross-border transfer разрешается только после юридической квалификации,
подтверждения местного юриста и настройки допустимого механизма
передачи.

Выбор локальных cloud/provider-площадок мотивирован не только законом:
он также нужен для latency, доступности Tier-1 ride APIs, требований
локальных партнёров, локальных платёжных интеграций и управления риском.
Техническая модель уже задана в [02-replication.md](02-replication.md),
[03-geo-routing.md](03-geo-routing.md) и [04-failover.md](04-failover.md):
`profile_region` хранит профиль/IAM/account data, `ride_region` хранит
операционную поездку, а межрегиональная репликация ограничивается
разрешёнными наборами данных.

## Требования

- Разделить архитектурные понятия: data residency, data localization,
  cross-border transfer, retention, deletion, data subject requests,
  audit и incident notification.
- Каждая доменная схема, Kafka-событие, таблица, витрина и ML feature
  должны иметь классификацию данных до production rollout.
- Передача за пределы региона/юридической зоны разрешается только после
  юридической квалификации и настройки допустимого механизма: contractual
  safeguards, consent/notice, approved processor/subprocessor, локальная
  DR-площадка или другой механизм, подтверждённый местного юриста.
- Точная геолокация считается чувствительной с точки зрения риска, даже
  если конкретная юридическая квалификация зависит от страны и контекста.
- Платёжные реквизиты не должны попадать в Kafka, логи и аналитическое
  хранилище в открытом виде; в событиях используются токены или ссылки на
  платёжную операцию.
- Секреты хранятся в Secrets Manager; данные шифруются in transit и at
  rest.
- В логах и трассировках запрещены полные номера карт, access tokens,
  пароли и избыточные точные персональные атрибуты.

## Решение

### 1. Compliance-модель как архитектурная policy

Для GoFuture используются следующие определения:

- **Data residency**: архитектурное правило о том, где хранится основной
  authoritative dataset для категории данных, например `profile_region`
  или `ride_region`.
- **Data localization**: более строгая policy, при которой обработка,
  хранение, backup/DR и доступ операторов ограничиваются локальной
  страной или юридической зоной. Применение такой policy требует
  подтверждения местного юриста для конкретного рынка.
- **Cross-border transfer**: любая передача данных, backup, DR-реплика,
  аналитический экспорт, support-доступ или model-training export за
  пределы исходной страны/юридической зоны. Передача разрешается только
  после юридической квалификации и настройки допустимого механизма.
- **Retention**: срок хранения по категории данных и цели обработки.
- **Deletion**: workflow удаления или обезличивания данных с учётом
  retention и legal hold.
- **Data subject requests (DSAR)**: процесс ответа на запросы субъекта
  данных: доступ, исправление, удаление, ограничение обработки, экспорт.
- **Audit**: неизменяемый след доступа и действий с чувствительными
  данными.
- **Incident notification**: процесс эскалации инцидентов безопасности и
  privacy incident review; сроки и адресаты уведомления определяются
  местного юриста.

### 2. Классификация данных

| Категория данных | Примеры | Архитектурная чувствительность |
|---|---|---|
| Identity/profile | `user_id`, имя, телефон, email, документы, настройки аккаунта, водительские атрибуты | Высокая: персональные данные; основной регион — `profile_region` |
| Поездки | `booking_id`, статусы, pickup/dropoff address, назначенный водитель, Saga state | Высокая: операционные данные услуги; основной регион — `ride_region` |
| Точная геолокация | GPS водителя/пассажира, location history, точные координаты pickup/dropoff | Очень высокая: чувствительная по риску; минимизация точности и срока хранения обязательна |
| Платёжные данные | payment method token, payment operation id, authorization/capture status, payout ledger | Очень высокая: PCI/финансовый риск; PAN/CVV не хранятся GoFuture |
| Fraud features | risk score, device/account signals, velocity counters, blacklist flags | Высокая: может включать персональные или поведенческие признаки |
| Агрегированная аналитика | counts/sums/rates по региону, `geo_cell`, часу, рынку, без прямых идентификаторов | Средняя/низкая при достаточной агрегации; риск реидентификации проверяется отдельно |
| Технические логи | structured app logs, error logs, audit logs | Средняя/высокая: не должны содержать секреты, токены и лишние PII |
| Метрики | RED/USE, Kafka/Flink lag, SLO, capacity metrics | Низкая, если labels не содержат PII и высокочувствительные идентификаторы |
| ML features | online/offline features для Pricing/Fraud, model training datasets | Зависит от состава: наследует максимальную чувствительность исходных полей |

### 3. Residency matrix

| Категория данных | Регион хранения | Допустимая репликация | Срок хранения | Шифрование | Доступ |
|---|---|---|---|---|---|
| Identity/profile | `profile_region` пользователя | Тонкий IAM-индекс (`user_id`, `tenant_id`, `profile_region`, роли без PII) глобально; profile projection только по allowlist и после legal review | Пока активен аккаунт + срок для поддержки/споров; удаление/анонимизация по DSAR с учётом legal hold | TLS/mTLS in transit; at rest KMS/managed encryption | Identity/Profile Service, support/admin через RBAC и audit |
| Поездки | `ride_region` конкретной поездки | Внутри региона multi-AZ; cross-border только после юридической квалификации или в виде агрегатов | Операционный срок + период претензий/финансовой сверки; точный срок задаётся retention policy | TLS/mTLS; at rest encryption; backups encrypted | Booking/Driver/Pricing/Fraud/Payments по сервисным границам |
| Точная геолокация | `ride_region` | Не экспортируется в точном виде; допускается coarse `geo_cell`/агрегаты после privacy review | Минимально необходимый срок для active ride, dispute, safety; затем агрегация/удаление | TLS/mTLS; at rest encryption; ограниченные indexes | Driver/Geography/Fraud realtime; доступ support только break-glass или ticket-based |
| Платёжные данные | `ride_region` и/или регион локального payment provider | PAN/CVV не хранятся; события содержат tokens/operation references; cross-border финансовые отчёты только агрегированы/юридически разрешены | По требованиям финансовой отчётности, chargeback и налогового учёта; legal hold при споре | TLS/mTLS; at rest encryption; provider tokenization; secret keys в Secrets Manager | Payments/Payouts, финансы и support по least privilege |
| Fraud features | `ride_region`; часть справочников глобальна | Aggregated/anonymized features в `core`; raw features cross-border только после legal review | По модели риска и anti-fraud policy; пересмотр TTL для поведенческих признаков | TLS/mTLS; at rest encryption | Fraud Service, data/ML pipelines по allowlist |
| Агрегированная аналитика | Региональный ClickHouse/Data Lake; глобальный `core` только для разрешённых агрегатов | Межрегионально только обезличенные/агрегированные витрины с проверкой k-anonymity/thresholds или эквивалентной policy | Дольше операционных данных, если нет риска реидентификации; срок задаётся BI/ML policy | TLS; at rest encryption | Analytics/BI роли, DataLens, data engineers |
| Технические логи | Локальный регион; централизованные security/audit views только после scrub/redaction | Cross-region допустим для очищенных логов без PII/secrets либо после legal qualification | Короткий операционный TTL; audit/security logs дольше по policy | TLS; at rest encryption; immutable storage для audit при необходимости | SRE/security/support по RBAC; break-glass audited |
| Метрики | Региональный monitoring; глобальные dashboards в `core` | Допустимы глобально при запрете PII в labels | По observability policy; обычно короче audit logs | TLS; at rest encryption | SRE/platform; no PII labels |
| ML features | Online store в регионе serving; offline store в региональном Data Lake | Model artifacts и агрегированные features могут распространяться после privacy/model risk review; raw training rows не экспортируются по умолчанию | По ML governance policy; пересмотр при изменении модели/цели | TLS; at rest encryption; access-controlled feature store | Data/ML, Pricing/Fraud owners по dataset grants |

### 4. Data residency, localization и cross-border transfer

Базовое архитектурное допущение:

- профиль, IAM-атрибуты и account data остаются в `profile_region`;
- операционные данные поездки, точная геолокация, Saga state, локальный
  Pricing, Fraud realtime и payment authorization остаются в
  `ride_region`;
- глобально реплицируются только справочники, конфигурация, схемы,
  feature flags, тонкий IAM-индекс и проверенные агрегаты;
- DR-копии операционных данных размещаются внутри той же юридической
  зоны либо создаются только после legal qualification.

Это согласуется с [02-replication.md](02-replication.md): cross-region
async replication имеет RPO по lag и применяется только к разрешённым
данным. Если местного юриста подтверждает допустимый механизм
cross-border transfer для конкретной категории данных, архитектура должна
явно зафиксировать scope, цель, срок, шифрование, получателя, subprocessor
и rollback/stop-transfer процедуру. До такого подтверждения поведение
консервативное: данные остаются в регионе, а для полного отказа региона
используется локальный/legal-zone DR или degraded mode из
[04-failover.md](04-failover.md).

### 5. Платёжные данные и PCI scope

Payments Service не хранит PAN или CVV. Ввод карточных данных выполняется
через hosted fields/SDK платёжного провайдера, а GoFuture получает только
`payment_method_id`, `payment_operation_id`, idempotency key, статус
авторизации/списания и ссылку на provider reconciliation record.

Платёжные реквизиты не попадают в Kafka, логи, трассировки, ClickHouse,
Data Lake и ML datasets в открытом виде. Kafka-события Payments содержат
только токены, ссылки на операцию, суммы, валюту, статус и технические
идентификаторы, необходимые для reconciliation. Ключи платёжных
интеграций, webhook secrets, DB credentials и mTLS certificates хранятся
в Secrets Manager; ротация и доступ к ним аудируются.

### 6. Logging, tracing и observability hygiene

Для логов, метрик и traces действует allowlist-подход:

- запрещены полные номера карт, CVV, access tokens, refresh tokens,
  session cookies, пароли, private keys и одноразовые коды;
- запрещены лишние точные персональные атрибуты: полный адрес, точная
  история GPS, документы, телефон/email, если они не нужны для конкретного
  audit/security события;
- `tenant_id`, `region_id`, `profile_region`, `ride_region`, `booking_id`,
  `saga_id`, `correlation_id` допустимы как технические идентификаторы,
  но labels метрик не должны создавать неконтролируемую cardinality или
  раскрывать PII;
- трассировки используют redaction/sampling; payload событий не пишется в
  span attributes целиком.

### 7. Data retention policy и legal hold

Retention задаётся не одним глобальным сроком, а policy по категории
данных и цели обработки:

- точная геолокация хранится минимально необходимое время: active ride,
  safety/dispute window, затем удаление или преобразование в coarse
  `geo_cell`/агрегат;
- события Kafka удерживаются по техническому retention из
  [Task2/02-kafka-topics.md](../Task2/02-kafka-topics.md), но не являются
  долгосрочным архивом персональных данных;
- региональный Data Lake хранит raw/validated события только в своём
  регионе и только для подтверждённой цели ML/BI/аудита;
- финансовые записи Payments/Payouts могут храниться дольше из-за
  отчётности, chargeback, налоговых или договорных требований;
- audit trail хранится отдельно от обычных app logs, с более строгой
  неизменяемостью и контролем доступа.

Legal hold приостанавливает deletion/anonymization для конкретных записей
или категорий, если есть спор, расследование, chargeback или регуляторный
запрос. Legal hold должен быть явным состоянием в metadata, иметь owner,
основание, срок пересмотра и аудит снятия.

### 8. Deletion workflow и DSAR

Запрос субъекта данных сначала маршрутизируется в `profile_region` через
Identity/Profile Service:

1. Identity проверяет заявителя и определяет `profile_region`.
2. Profile Service формирует инвентарь связанных данных: профиль,
   account data, payment tokens, активные/исторические поездки по
   `ride_region`, support tickets, audit records.
3. Для профиля выполняется удаление или анонимизация прямых
   идентификаторов, если нет legal hold.
4. Для поездок в других `ride_region` отправляются административные
   команды на анонимизацию разрешённых операционных следов. Это не
   требует прямого доступа к чужим БД и сохраняет Database-per-Service.
5. Финансовые и audit records не удаляются полностью, если retention/legal
   hold требует хранения; вместо этого удаляются или заменяются прямые
   идентификаторы там, где это допустимо.
6. Пользователь получает результат DSAR с указанием, какие данные удалены,
   какие обезличены, какие сохранены из-за legal hold/retention.

DSAR workflow должен иметь SLA обработки, retry, DLQ/ручную очередь для
ошибок, журнал решений и регулярную сверку, что команды удаления дошли до
нужных `ride_region`.

### 9. Audit trail и break-glass access

Доступ к чувствительным данным из админ-панели, support-инструментов,
data-пайплайнов и production debugging логируется неизменяемо:

- кто получил доступ: user/service account, роль, tenant, регион;
- к чему был доступ: категория данных, record reference, не полный payload;
- зачем: ticket/incident/legal basis;
- когда: timestamp, source IP/device posture, approval chain;
- что сделано: read/export/update/delete/anonymize/break-glass.

Break-glass access разрешён только для инцидентов и критичной поддержки:
короткий TTL, явное одобрение, MFA, запись причины, автоматическое
уведомление security/compliance owner и обязательный post-review. Общие
технические учётные записи с прямым доступом к сервисным БД не являются
допустимым механизмом штатного доступа.

### 10. Incident notification

Архитектура должна собирать достаточно данных для privacy incident review:
затронутые категории данных, регионы, tenants, примерный объём записей,
путь утечки, время обнаружения и containment. Решение о внешнем
уведомлении пользователей, регуляторов, партнёров или платёжных схем
принимается не архитектурой, а incident/legal процессом; сроки и адресаты
требуют подтверждения местного юриста.

## Альтернативы

- **Утверждать абсолютный запрет cross-border transfer по названию закона.**
  Отклонено: это юридическое заключение, а не архитектурный дизайн. ADR
  фиксирует консервативное архитектурное допущение и legal gate для
  передачи данных.
- **Единое глобальное хранение всех персональных и операционных данных в
  `core`.** Отклонено: ухудшает latency, повышает blast radius утечки,
  усложняет партнёрские требования и противоречит региональному
  шардингу из [02-replication.md](02-replication.md).
- **Полный собственный PCI DSS scope с хранением PAN/CVV.** Отклонено:
  несоразмерно увеличивает аудит и риски. Токенизация у локального
  провайдера покрывает платёжный сценарий без хранения карточных данных
  в GoFuture.
- **Считать анонимизацию чисто технической задачей.** Отклонено:
  достаточность агрегации/обезличивания зависит от набора полей, размера
  групп, риска реидентификации и локальной правовой квалификации.

## Компромиссы и риски

- Консервативная policy может ограничить скорость запуска analytics/ML и
  DR-сценариев до получения legal qualification.
- Allowlist для profile projections, analytics exports и ML features
  требует постоянного сопровождения; Schema Registry проверяет структуру,
  но не юридическую допустимость поля.
- Анонимизация вместо полного удаления для записей с mandatory retention
  требует понятного пользовательского ответа по DSAR.
- Аварийный привилегированный доступ необходим при инцидентах, но повышает риск злоупотреблений, поэтому все его применения должны строго регистрироваться и регулярно проверяться.
- Локальные cloud/provider-площадки уменьшают latency и регуляторный риск,
  но повышают стоимость, операционную сложность и число subprocessors,
  требующих vendor risk review.

## Статус

Предложено. Требует подтверждения местного юриста по каждому рынку,
категории данных и механизму cross-border transfer перед production launch.
