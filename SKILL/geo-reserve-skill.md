# Multi-Region Архитектура и Георезервирование в Сбербанке

> **Источник**: внутренние документы Сбербанка (КТН, RMS), анализ репозиториев `platform-files`, `sbercom-store`.
> Общение с пользователем — **только на русском языке**.

---

## 1. Что такое Георезервирование?

**Георезервирование** — это дублирование инфраструктуры приложения в нескольких географических регионах (центрах обработки данных, ЦОД) с целью обеспечения устойчивости к сбоям в одном регионе.

### Цели требования:
- Минимизировать время восстановления (RTO)
- Обеспечить непрерывность работы при сбое ЦОД
- Соответствовать внутренним стандартам надёжности (КТН)

### Пороги соответствия (актуально на 2026 год):
| Дата | Порог автопроверки | Что означает |
|------|-------------------|--------------|
| Июль 2026 | **90%** | Минимум 91% ресурсов должно быть дублировано |
| Октябрь 2026 | **100%** | Все ресурсы должны быть в резервном регионе |

---

## 2. Архитектурные модели в Сбербанке

### 2.1 Federated Model (Федеративная архитектура)

Сбербанк использует **4 федерации** (сегменты сети):

```go
// internal/federation/names.go
const (
    PAOSigma = "PAO_SIGMA"   // Основная федерация (Москва)
    PAOAlpha = "PAO_ALPHA"   // Резервная федерация (Екатеринбург)
    DZO      = "DZO"         // Дальневосточный ОК
    SBT      = "SBT"         // Субъекты
)
```

**Как работает:**
- Каждый деплой сервиса работает в **одной федерации**
- Конфигурация: `federation: PAO_SIGMA` в `config.yaml`
- Выбор хранилища через `storageresolver.Bind` по коду федерации

### 2.2 Active-Active (Активный-Активный)

Оба региона работают одновременно:
```
                    ┌─────────────────┐
                    │  GeoDNS/GSLB    │
                    │ (F5/Cloudflare) │
                    └────────┬────────┘
                             │
          ┌──────────────────┼──────────────────┐
          │                  │                  │
    ┌─────▼─────┐      ┌─────▼─────┐    ┌─────▼─────┐
    │  МСК (A)  │      │  ЕКБ (B)  │    │ Дальневост.│
    │  Актив    │      │  Актив    │    │  Актив     │
    └─────┬──────┘      └─────┬──────┘    └─────┬──────┘
         │                   │                  │
    ┌────▼────┐         ┌────▼────┐      ┌────▼────┐
    │  App    │         │  App    │      │  App    │
    └────┬────┘         └────┬────┘      └────┬────┘
         │                   │                  │
    ┌────▼────┐         ┌────▼────┐      ┌────▼────┐
    │  DB R1  │───────► │  DB R2  │──────► │  DB R3  │
    │ Primary │ Sync/Async│Secondary│      │ Backup  │
    └─────────┘           └─────────┘      └─────────┘
```

---

## 3. Техническая реализация (пример: platform-files)

### 3.1 Двойная репликация S3

**Конфигурация:**

```yaml
# config.example.yaml
s3:
  default_connection: default
  connections:
    - s3_name: default
      endpoint: "http://localhost:9000"    # Primary (МСК)
      region: us-east-1
      access_key_id: minioadmin
      secret_access_key: minioadmin
      force_path_style: true
    - s3_name: mirror
      endpoint: "http://localhost:9020"    # Mirror (ЕКБ)
      region: us-east-1
      access_key_id: minioadmin
      secret_access_key: minioadmin
      force_path_style: true
```

**Механизм репликации:**

```go
// internal/replication/after_commit.go
func AfterPrimaryS3Complete(ctx context.Context, lg *zap.Logger, in AfterCommitInput) error {
    // 1. Помечает primary file_storage READY
    p0 := in.Ordered[0]
    p0.Status = models.FileStorageStatusReady
    p0.UpdatedAt = time.Now()
    in.FileStore.Update(ctx, &p0)

    // 2. Копирует объект в резервные S3 хранилища
    for i := 1; i < len(in.Ordered); i++ {
        // Копирование из src в dst
        s3.ReplicateObject(ctx, lg, src, dst, srcBucket, dstBucket, key, size, chunkSize)
    }
}
```

**Параметры:**
- `upload_replication_factor` — количество копий файла (максимум 32)
- `ReplicationChunkSizeBytes` — размер чанка при копировании (16MB по умолчанию)

### 3.2 federated Базы Данных

**Docker Compose:** две независимых БД

```yaml
# docker-compose.yml
services:
  postgres:
    environment:
      POSTGRES_DB: files              # Federated A
  # ...
  postgres_fed_b:
    environment:
      POSTGRES_DB: files_fed_b        # Federated B
```

**Конфигурация:**

| Параметр | Federated A | Federated B |
|----------|-------------|-------------|
| `database.name` | `files` | `files_fed_b` |
| `server.port` | `8080` | `8081` |
| `federation` | `PAO_SIGMA` | `PAO_ALPHA` |
| `kafka.consumer_group` | `follower-a` | `follower-b` |

### 3.3 Синхронизация через Kafka

**Правила копирования:**

```go
// internal/federation/rule.go
type Rule struct {
    ID                 uuid.UUID
    SourceFederation   string    // откуда (PAO_SIGMA)
    TargetFederation   string    // куда (PAO_ALPHA)
    DlpRequired        bool      // обязательная проверка DLP
    ExtensionWhitelist []string  // допустимые расширения
    MaxSize            int64     // максимальный размер
    RequiredCheckers   []string  // обязательные проверки
    Enabled            bool
}
```

**Поток синхронизации:**
1. Файл загружен в Federated A → событие `FileSyncCreated` в Kafka
2. Consumer Federated B читает событие через отдельную `consumer_group`
3. Копирует бинарник из primary S3 A в mirror S3 B
4. Создаёт запись `file_storage` в БД B

---

## 4. Требования к приложению для прохождения георезервирования

### 4.1 Технические требования

| Требование | Описание | Пример |
|------------|----------|--------|
| **Stateless** | Приложение не хранит состояние в памяти | Сессии в Redis, реплицируемом между регионами |
| **S3 репликация** | Каждый объект хранится в ≥2 S3 хранилищах | MinIO primary + MinIO replica |
| **DB репликация** | База данных или БД в каждом регионе | Две PostgreSQL БД для разных федераций |
| **Kafka** | Синхронизация событий между регионами | `platform-files-events` топик |
| **Health Checks** | Проверка доступности каждого региона | `/health/live` для MinIO |
| **Load Balancer** | Переключение трафика при сбое | F5 GSLB или Kubernetes Service |

### 4.2 Расчёт процента георезервирования

```
Процент = (Количество ресурсов в резервном регионе / Общее количество ресурсов) × 100%
```

**Пример:**

| Компонент | Primary | Secondary | Включён в процент? |
|-----------|---------|-----------|--------------------|
| S3 хранилище | 1 | 1 | ✅ (100%) |
| База данных | 1 | 1 | ✅ (100%) |
| HTTP API инстансы | 2 | 2 | ✅ (100%) |
| Kafka топики | 3 реплики | 3 реплики | ✅ (100%) |

**Итого: 100%** — требование пройдено (≥91%)

### 4.3 Что НЕ проходит требования

| ❌ Проблема | Почему не проходит |
|------------|---------------------|
| Сессии в памяти (in-memory) | При фейловере все сессии теряются → RTO = восстановление всех сессий |
| Single-point S3 | Один MinIO без резервной копии → потеря данных при сбое диска |
| Single БД | Одна PostgreSQL в одном регионе → полная остановка при сбое ЦОД |
| Sticky sessions на LB | Пользователь привязан к конкретному серверу → не переключается при сбое |
| Нет health checks | Система не знает, что регион упал → не переключает трафик |

---

## 5. Как проверить соответствие в RMS/M-App

### 5.1 Шаги проверки:

1. Зайти в **RMS** или **M-App** (система управления требованиями)
2. Найти свою ИТ-услугу
3. Перейти к требованию **«Георезервирование»**
4. Посмотреть **Процент георезервирования** в комментариях/полях требования
5. Сравнить с порогом (90% → должно быть **≥91%**)

### 5.2 Где смотреть в коде:

| Файл | Что искать |
|------|------------|
| `config.yaml` | Два `s3.connections` с разными `s3_name` (`default` и `mirror`) |
| `docker-compose.yml` | Два сервиса `minio` и `minio-replica`, две БД |
| `internal/federation/names.go` | Константы `PAOSigma`, `PAOAlpha`, `DZO`, `SBT` |
| `internal/replication/after_commit.go` | Функция `AfterPrimaryS3Complete` — копирование в mirror |

---

## 6. Конфигурация Kafka для multi-region

### 6.1 Producer конфигурация (для надёжной доставки):

```yaml
# application.yml (Spring Boot) или аналог
spring:
  kafka:
    producer:
      acks: all                        # Ждать подтверждения от ВСЕХ ISR-реплик
      retries: 2147483647              # Максимум
      properties:
        enable.idempotence: true       # Ровно одна запись, без дублей
        max.in.flight.requests.per.connection: 5
        delivery.timeout.ms: 120000    # 2 минуты
```

### 6.2 Topic конфигурация:

```properties
# Настройки топика
replication.factor=3              # Минимум для production (N-1 отказов без потерь)
min.insync.replicas=2             # Запись только если ≥2 реплик синхронизированы
unclean.leader.election.enable=false  # Запрет назначать лидером отставшую реплику
log.retention.hours=168           # 1 неделя хранения
```

### 6.3 MirrorMaker 2 для межкластерной репликации:

```yaml
# MirrorMaker 2 конфигурация
clusters = source, target

source.bootstrap.servers = source-kafka:9092
target.bootstrap.servers = target-kafka:9092

source->target.enabled = true
source->target.topics = .*         # Все топики
source->target.groups = .*         # Все consumer groups

replication.factor = 3
checkpoints.topic.replication.factor = 3
```

---

## 7. Чеклист при реализации multi-region архитектуры

### Producer
- [ ] `acks=all` + `enable.idempotence=true` для критичных данных
- [ ] Ключ сообщения = атрибут группировки (гарантирует упорядоченность)
- [ ] Асинхронная отправка с обработкой ошибки в callback
- [ ] Тест: что происходит если брокер недоступен?

### Consumer
- [ ] `enable.auto.commit=false` — ручной коммит
- [ ] `@RetryableTopic` с DLT для business-critical топиков
- [ ] Разделены Retryable и Non-Retryable исключения
- [ ] `concurrency` ≤ числа партиций топика
- [ ] `max.poll.interval.ms` > реального времени обработки
- [ ] Consumer идемпотентен (at-least-once = возможны дубли)

### Топик
- [ ] `replication.factor=3` (production)
- [ ] `min.insync.replicas=2`
- [ ] Выбрано число партиций (≥ ожидаемого кол-ва consumer'ов)
- [ ] `retention` настроен под SLA (данные нужны для повтора?)
- [ ] Naming convention: `{домен}.{сущность}.{событие}`

### Тестирование
- [ ] `@EmbeddedKafka` для unit/integration тестов
- [ ] Тест happy-path: сообщение доставлено и обработано
- [ ] Тест retry: сообщение попало в DLT после исчерпания retry
- [ ] Тест: consumer корректно обрабатывает дубли

---

## 8. Распространённые ошибки и решения

| Ошибка | Причина | Решение |
|--------|---------|---------|
| Consumer постоянно rebalance'ится | `max.poll.interval.ms` слишком мал или обработка медленная | Увеличить `max.poll.interval.ms`; ускорить обработку |
| Дублирование сообщений | Consumer завершился до commit offset | Сделать обработку идемпотентной (upsert, unique key) |
| Потеря сообщений | `acks=0` или `acks=1` при failover | Переключить на `acks=all` + `min.insync.replicas=2` |
| Producer зависает | `buffer.memory` переполнен | Увеличить `buffer.memory` или уменьшить `max.block.ms` |
| Рост lag DLT | Бизнес-ошибки попадают в retry | Исключить Non-Retryable exceptions из retry |
| Нарушение порядка | Несколько partition для одной сущности | Использовать ключ = ID сущности |
| NotEnoughReplicasException | `min.insync.replicas > доступных ISR` | Восстановить брокеры или снизить `min.insync.replicas` временно |

---

## 9. Полезные ссылки

- **RMS** — система управления требованиями (внутренняя)
- **M-App** — мобильное приложение для проверки требований
- **СберЧат канал** — анонсы изменений в КТН
- **Сбер911** — система управления инцидентами

---

*Документ актуален на 07.07.2026. Дата повышения порога георезервирования: октябрь 2026 → 100%.*
