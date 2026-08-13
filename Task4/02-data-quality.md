# ADR-T4-02: Обеспечение качества данных

Дата: 2026-07-17
Авторы: Архитектурный комитет GoFuture, data-инженеры

## Контекст

Пайплайн данных ([01-data-pipeline.md](01-data-pipeline.md)) питает не
только BI (DataLens), но и ML-модели, влияющие на реальные деньги
(динамическое ценообразование) и решения о блокировке пользователей
(обнаружение мошенничества). Ошибка в данных на любом этапе — от
публикации события до подачи признака в модель — может тихо испортить
цену поездки или несправедливо заблокировать пассажира/водителя, и без
специальных механизмов будет обнаружена только постфактум, по жалобам или
по деградации бизнес-метрик. Нужен явный набор механизмов качества данных
на каждом этапе пайплайна.

## Требования

- Некорректная схема события не должна попадать в Kafka вообще — раньше,
  чем она дойдёт до любого потребителя (согласуется с BACKWARD
  compatibility Schema Registry, [Task2/02-kafka-topics.md](../Task2/02-kafka-topics.md)).
- Владеющая доменом команда и data-инженеры должны иметь явное, а не
  подразумеваемое соглашение о том, какие данные публикуются, в каком
  формате и с какими гарантиями.
- Невалидные записи не должны ни блокировать пайплайн (тот же принцип, что
  и для DLQ, [Task2/05-delivery-guarantees.md](../Task2/05-delivery-guarantees.md)),
  ни молча теряться.
- Деградация качества данных и дрейф данных для ML должны обнаруживаться
  проактивно, через метрики и алерты, а не по жалобам пользователей или
  постфактум-анализу инцидента.
- Любую запись в Data Lake/ClickHouse/Feature Store должно быть можно
  проследить до исходного события — необходимо для отладки и для аудита
  анонимизирующего экспорта ([01-data-pipeline.md](01-data-pipeline.md),
  [Task3/05-compliance.md](../Task3/05-compliance.md)).
- Ключевые витрины и feature sets должны иметь измеримый quality score,
  freshness SLA, владельца и алерты; DataLens не должен читать сырые Kafka
  topics или raw PII.

## Решение

Набор механизмов качества строится вокруг data contracts, автоматических
проверок, quarantine/DLQ, replay, lineage, data catalog, quality score и
алертов. Проверки применяются к raw restricted zone, pseudonymized
operational analytics и anonymized aggregated datasets из
[01-data-pipeline.md](01-data-pipeline.md).

### Data quality dimensions

| Dimension | Проверка | Инструмент | Порог | Реакция | Владелец |
|---|---|---|---|---|---|
| Schema validity | Событие соответствует Avro-схеме и BACKWARD compatibility; обязательные envelope-поля есть | Schema Registry, producer contract tests, CI | 100% валидных событий; breaking change = 0 | Reject produce/merge, откат схемы, алерт владельцу домена | Доменная команда-продюсер + platform |
| Completeness | Обязательные бизнес-поля не пусты: `booking_id`, `event_id`, `occurred_at`, `region_id`, суммы/валюта для Payments | Great Expectations/dbt tests, Flink validation | ≥ 99,9% для operational events; 100% для Payments/Payouts critical fields | Невалидные записи в quarantine, алерт, replay после исправления | Доменная команда + data engineers |
| Uniqueness | Нет дублей `event_id`; idempotency keys уникальны в окне | Flink dedup state, ClickHouse/dbt uniqueness tests | Дубли сверх ожидаемых retry ≤ 0,1%; для payment idempotency конфликт = 0 | Dedup, quarantine конфликтов, incident для Payments | Data engineers; Payments owner для money flows |
| Validity | Значения в допустимых диапазонах: координаты, `geo_cell`, цены > 0, currency ISO, `surge_multiplier` в policy | Great Expectations, custom Flink rules | Нарушения ≤ 0,1%; финансовые суммы/currency = 0 нарушений | Quarantine, DLQ события правил, rollback/replay producer fix | Data engineers + доменный owner |
| Consistency | Согласованность статусов Saga, price/payment amounts, region/tenant, `ride_region` в событиях одной поездки | Flink joins, dbt tests, reconciliation jobs | Несогласованные ключевые факты = 0 для Booking/Payments; ≤ 0,1% для аналитики | Stop affected mart, алерт, reconciliation, replay | Booking/Payments/Pricing owners + data engineers |
| Timeliness/Freshness | Лаг event time → curated/BI/Feature Store; watermark lag; batch completion time | Prometheus/Grafana, Flink metrics, freshness checks | Tier-1 online features p95 ≤ 2 мин; regional BI ≤ 5 мин; global BI ≤ 30 мин | Alertmanager, autoscale/restart job, mark dashboard stale | Data engineers + SRE |
| Referential integrity | `booking_id`, `driver_id`, `payment_id`, `tenant_id` существуют в допустимых источниках/справочниках | dbt relationships, Flink async lookup/read models | Orphan records ≤ 0,1%; Payments orphans = 0 | Quarantine orphans, backfill lookup, replay after source fix | Data engineers + source domain |
| Distribution drift | PSI/KS/mean/std drift для ML features и BI ключевых метрик по региону/тенанту | Feature Store monitoring, Evidently/custom jobs, Grafana | Warning PSI > 0,1; critical PSI > 0,25 или domain-specific threshold | Алерт, freeze promotion, retraining/recalibration, data incident review | ML/data owners + Pricing/Fraud teams |

Quality score витрины рассчитывается как взвешенная сводка этих измерений
с учётом критичности dataset. Dataset с score ниже порога не публикуется в
serving layer для DataLens/ML либо помечается как stale/degraded.

### 1. Schema Registry как обязательный контракт

- **Этап пайплайна**: момент публикации события продюсером, до попадания
  в Kafka (самая ранняя возможная точка контроля).
- **Проблема, которую закрывает**: событие со схемой, не прошедшей
  проверку совместимости (BACKWARD, [Task2/02-kafka-topics.md](../Task2/02-kafka-topics.md)),
  не публикуется вовсе — попытка `produce` отклоняется на уровне клиента
  Schema Registry. Это устраняет целый класс проблем качества данных
  (пропущенные поля, несовместимые типы), не давая им физически попасть в
  систему, а не пытаясь отловить их позже в пайплайне.

### 2. Data contracts между командами-владельцами доменов и data-инженерами

- **Этап пайплайна**: до появления нового события/поля в каталоге
  ([Task2/01-domain-events.md](../Task2/01-domain-events.md)) или до
  настройки нового источника CDC — процесс проектирования, предшествующий
  любому коду.
- **Проблема, которую закрывает**: Schema Registry проверяет только
  структурную совместимость, но не смысл поля, не гарантии свежести/полноты
  и не то, что произойдёт при депрекации поля. Data contract — явное
  соглашение (владелец, семантика полей, гарантии по свежести/полноте,
  privacy classification, lineage, допустимые consumers, SLA витрин,
  политика изменения/депрекации) между доменной командой-продюсером и
  data-инженерами — закрывает разрыв, который остаётся, даже если схема
  формально валидна, но её смысл трактуется по-разному двумя сторонами.

Минимальный data contract включает:

- owner доменного события/CDC-источника и owner downstream dataset;
- схему, версию, compatibility mode и ссылку на Schema Registry subject;
- business semantics каждого поля и допустимые значения;
- freshness SLA, completeness target и правила поздних событий;
- privacy classification: raw, pseudonymized, anonymized, sensitive PII,
  precise geolocation, payment reference;
- правила masking/pseudonymization/anonymization;
- contract tests в CI, план депрекации и replay/backfill instructions.

### 3. Валидация в пайплайне (Great Expectations / dbt tests)

- **Этап пайплайна**: внутри регионального Flink-пайплайна
  ([01-data-pipeline.md](01-data-pipeline.md)), сразу после потребления
  события/CDC-записи, до записи в ClickHouse/Data Lake.
- **Проблема, которую закрывает**: Schema Registry гарантирует
  структурную форму, но не бизнес-корректность значений. Проверяются:
  **полнота** (обязательные поля непусты — например, `booking_id` есть в
  каждом событии саги), **диапазоны** (например, `final_price` > 0,
  координаты в пределах допустимых широты/долготы, `surge_multiplier` в
  разумных пределах), **уникальность ключей** (например, отсутствие
  дублей `event_id` сверх ожидаемого at-least-once, согласуется с
  идемпотентностью потребителей, [Task2/05-delivery-guarantees.md](../Task2/05-delivery-guarantees.md)).
  Это первая линия защиты от "тихо неверных, но структурно валидных"
  данных.

### 4. DQ-метрики в Prometheus/Grafana

- **Этап пайплайна**: сквозной мониторинг всего пайплайна (приём,
  обработка, запись), а не отдельная стадия — расширяет уже
  эксплуатируемый стек ([Task2/06-monitoring.md](../Task2/06-monitoring.md)).
- **Проблема, которую закрывает**: механизмы 1–3 обнаруживают проблему в
  момент её возникновения на конкретной записи; DQ-метрики дают
  агрегированную, наблюдаемую во времени картину — **свежесть данных**
  (лаг между временем события и временем появления в
  ClickHouse/Data Lake — деградация свежести означает, что модели и
  дашборды работают на устаревших данных, даже если сами записи
  корректны), **доля отбраковки** (% записей, не прошедших валидацию из
  п. 3 — рост доли сигнализирует о проблеме у конкретного продюсера до
  того, как это станет заметно в качестве модели), **дубликаты** (доля
  записей, отфильтрованных дедупликацией, — рост может указывать на
  проблему в retry-логике продюсера, [Task2/05-delivery-guarantees.md](../Task2/05-delivery-guarantees.md)).
  Алерты — через существующий Alertmanager.

Для ключевых витрин фиксируются SLA:

- online Feature Store для Pricing/Fraud: freshness p95 ≤ 2 минуты,
  availability не ниже Tier-1 зависимости соответствующего региона;
- региональные operational BI views: freshness p95 ≤ 5 минут;
- глобальные anonymized BI views в `core`: freshness p95 ≤ 30 минут;
- financial reconciliation marts: закрытие daily batch до T+1 06:00
  локального времени региона.

Алерты срабатывают на freshness breach, рост quarantine/DLQ, падение
quality score, drift critical threshold, lineage gap и отсутствие данных
от критичного producer.

### 5. Карантинная зона для невалидных записей

- **Этап пайплайна**: параллельный путь сразу после валидации (п. 3) —
  запись, не прошедшая проверку, не отбрасывается и не блокирует
  обработку остальных записей той же партиции (тот же принцип, что и DLQ
  для доставки событий, [Task2/05-delivery-guarantees.md](../Task2/05-delivery-guarantees.md),
  применённый теперь к содержимому, а не к факту доставки).
- **Проблема, которую закрывает**: без карантинной зоны есть только два
  плохих варианта — либо молча отбросить невалидную запись (потеря
  данных, скрытая деградация полноты), либо заблокировать ею весь
  пайплайн (недоступность для остальных, корректных записей). Карантинная
  зона (отдельная область/таблица в Data Lake) сохраняет запись для
  разбора и последующей переобработки после исправления причины (в коде
  продюсера, в правиле валидации, и т. п.), не жертвуя ни целостностью
  учёта, ни доступностью пайплайна.

Quarantine хранит исходную запись, причину отказа, версию схемы, job
version, validation rule id, `event_id`, `correlation_id`, `region_id` и
dataset owner. После исправления причины данные переобрабатываются через
контролируемый replay из Kafka/Data Lake с теми же idempotency rules, что
и обычный пайплайн. Если запись не может быть исправлена автоматически,
она остаётся в ручной очереди разбора; удаление из quarantine без решения
владельца запрещено.

### 6. Data lineage (OpenLineage)

- **Этап пайплайна**: сквозной — каждая стадия обработки (Flink-джобы,
  анонимизирующий экспорт, обучение моделей, см.
  [01-data-pipeline.md](01-data-pipeline.md)) публикует метаданные о том,
  что из чего получено.
- **Проблема, которую закрывает**: при обнаружении проблемы (деградация
  качества данных, деградация модели, вопрос аудита "какие именно данные
  и в каком виде пересекли границу региона при анонимизирующем экспорте")
  нужно быстро проследить конкретную запись/поле назад до исходного
  события/топика/версии схемы. Без lineage это — ручное расследование по
  логам; с OpenLineage — прослеживаемый граф происхождения данных,
  критичный как для отладки ([Task2/06-monitoring.md](../Task2/06-monitoring.md)
  MTTR, требование Э1, [Task1/01-nfr.md](../Task1/01-nfr.md)), так и для
  аудита доступа/обработки персональных данных
  ([Task3/05-compliance.md](../Task3/05-compliance.md)).

Lineage публикуется в data catalog вместе с dataset ownership, privacy
classification, retention class, quality score, SLA и ссылкой на data
contract. Это позволяет DataLens, ML Training и downstream jobs выбирать
только curated/serving datasets, а не случайно читать raw restricted zone.

### 7. Мониторинг дрейфа данных для ML-моделей

- **Этап пайплайна**: на границе Feature Store ↔ Model Serving и в цикле
  переобучения ([01-data-pipeline.md](01-data-pipeline.md)) — сравнение
  статистического распределения признаков в реальном времени (онлайн) с
  распределением на момент обучения (офлайн).
- **Проблема, которую закрывает**: модель может тихо деградировать без
  единой ошибки — например, изменение распределения кодов отказа платежа
  после смены локального провайдера ([Task3/05-compliance.md](../Task3/05-compliance.md))
  или сдвиг паттернов спроса — механизмы 1–6 не обнаружат это, так как
  сами данные остаются структурно и по бизнес-правилам валидными.
  Мониторинг дрейфа — единственный механизм, нацеленный именно на
  "данные корректны, но статистически другие, чем модель видела при
  обучении", и служит триггером для переобучения через петлю обратной
  связи ([01-data-pipeline.md](01-data-pipeline.md)).

Для ML дополнительно обязательны:

- **training-serving skew protection**: одно определение признака для
  offline/online, автоматические parity tests и сравнение sample outputs;
- **feature freshness**: SLA по каждому online feature, stale features
  исключаются или приводят к fallback модели/правилу;
- **feature lineage**: связь feature → source event/CDC → transform code
  → dataset snapshot;
- **drift monitoring**: population/data drift по region/tenant/model
  segment, отдельные thresholds для Pricing и Fraud;
- **model/data versioning**: каждая prediction содержит `model_version`,
  `feature_set_version`, training dataset snapshot и transform version.

### 8. Curated serving layer для BI

DataLens читает только curated/serving layer: regional pseudonymized views
для ограниченной операционной аналитики и anonymized aggregated views для
глобального BI. Прямой доступ DataLens к raw Kafka topics, raw Data Lake,
PII mapping/vault и точным GPS-таблицам запрещён на уровне IAM и data
catalog policy. Если витрина не прошла quality threshold или privacy
classification, она не публикуется в BI workspace.

## Альтернативы

- **Валидация только на входе (Schema Registry), без проверок бизнес-правил
  в пайплайне.** Отклонено: структурно валидное событие может быть
  бизнес-некорректным (отрицательная цена, координаты вне допустимого
  диапазона) — Schema Registry физически не может проверить это, отсюда
  необходимость отдельного слоя валидации (механизм 3).
- **Отбрасывать невалидные записи молча, без карантинной зоны.**
  Отклонено: приводит к незаметной потере полноты данных — то же
  рассуждение, что уже привело к выбору DLQ вместо блокирующего retry в
  [Task2/05-delivery-guarantees.md](../Task2/05-delivery-guarantees.md),
  применённое здесь к содержимому данных.
  Полагаться только на DQ-метрики (агрегированные) без построчного
  lineage. Отклонено: метрики сигнализируют, ЧТО деградировало (например,
  выросла доля отбраковки), но не отвечают на вопрос, ГДЕ именно в графе
  трансформаций находится причина — для этого нужен lineage, метрики и
  lineage дополняют друг друга, а не заменяют.
- **Мониторить только точность модели (accuracy/precision постфактум) без
  отдельного мониторинга дрейфа входных данных.** Отклонено: точность
  модели на реальных исходах становится известна с задержкой (нужно
  дождаться фактического результата поездки/платежа); мониторинг дрейфа
  признаков даёт более раннее предупреждение, до того как деградация
  проявится в бизнес-метриках.
- **Разрешить BI читать raw Data Lake/Kafka напрямую, полагаясь на права
  пользователей.** Отклонено: это обходит privacy transformations,
  quality gates, lineage и data catalog. BI должен работать с curated
  serving layer.

## Компромиссы и риски

- Data contracts (механизм 2) — процессный, не автоматизируемый полностью
  механизм; его соблюдение зависит от дисциплины команд, а не только от
  тулинга — требует периодического ревью наравне с contract-тестами
  API/событий ([Task1/04-backward-compatibility.md](../Task1/04-backward-compatibility.md)).
- Валидация в пайплайне (механизм 3) добавляет вычислительную нагрузку и
  латентность обработки внутри Flink-джобов — для онлайн-признаков,
  идущих в Model Serving с жёстким latency-бюджетом
  ([01-data-pipeline.md](01-data-pipeline.md)), объём проверок должен быть
  соразмерен бюджету, а не исчерпывающим набором правил любой ценой.
- Карантинная зона без выделенного процесса разбора превращается в то же
  "кладбище", что и DLQ без регламента
  ([Task2/05-delivery-guarantees.md](../Task2/05-delivery-guarantees.md)) —
  нужен эксплуатационный регламент по разбору карантина, а не только
  наличие механизма.
- OpenLineage требует, чтобы каждая стадия пайплайна (включая уже
  существующие Flink-джобы из [Task2/03-stream-processing.md](../Task2/03-stream-processing.md))
  была дополнена публикацией lineage-метаданных — это ретроактивное
  изменение уже спроектированных джобов, а не только требование к новым.
- Мониторинг дрейфа данных сам по себе требует хранения статистических
  снапшотов обучающих данных для сравнения — дополнительный объём
  хранения в Feature Store/Data Lake, который нужно учитывать при
  планировании ёмкости наравне с ростом от петли обратной связи
  ([01-data-pipeline.md](01-data-pipeline.md)).

## Статус

Предложено.
