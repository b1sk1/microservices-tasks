# Task 1 — Lender Limit Blocking + Outbox

## Условие

```java
// Task
// Implement blockLenderLimit(id).
// expected initial status: SCORING_APPROVED
// expected updated status: LIMIT_BLOCKED
//
// The method must block the requested credit amount in an external lender system.
// After successful blocking, the service must send ApplicationLimitBlocked event to Kafka.

import java.math.BigDecimal;
import java.util.Optional;
import java.util.UUID;

enum ApplicationStatus {
    INITIAL,
    SCORING_APPROVED,
    LIMIT_BLOCKED,
    CONTRACT_SIGNED,
    REJECTED
}

record Application(
        UUID id,
        UUID clientId,
        UUID lenderId,
        ApplicationStatus status,
        BigDecimal requestedAmount,
        Optional<String> lenderBlockId
        // more fields...
) {}

record LenderBlockResult(
        String blockId
) {}

interface ApplicationRepository {
    // methods
}

interface LenderClient {
    LenderBlockResult blockLimit(
            UUID lenderId,
            UUID applicationId,
            BigDecimal amount,
            String requestId
    );
}

public class LedgerService {
    // TODO
}

public class OutboxMessageProducer {
    // TODO – extra task, not requered, after LedgerService done.
}

interface KafkaProducer {
    void send(String topic, String key, String payloadJson);
}

interface ApplicationService {

}

interface ApplicationController {

}
```

Задача: спроектировать сервис с одним методом, как в описании. Дополнительно продумать применение outbox-решения.

Решение — в [`LedgerService.java`](./LedgerService.java).

## Ключевые решения

- **Идемпотентность через requestId.** `requestId` детерминированно вычисляется 
  из `applicationId` (а не рандомно), поэтому повторный вызов метода 
  (ретрай, дублирующийся запрос) безопасен: лендер распознаёт дубликат и 
  вернёт тот же blockId, а не заблокирует лимит дважды.
- **Внешний вызов вне транзакции БД.** Вызов `lenderClient.blockLimit(...)` 
  происходит без открытой транзакции; сохранение `Application` + запись в outbox 
  атомарны между собой (одна короткая транзакция через `TransactionTemplate`).
- **Уже LIMIT_BLOCKED → success, а не ошибка.** Трактуется как идемпотентный 
  успешный результат повторного вызова.
- **Outbox** реализован как отдельная таблица + polling-publisher 
  (`OutboxMessageProducer` / `MessageService`), с ретраями и очисткой старых записей. 
  В проде вместо polling лучше Debezium/CDC.

## Известные упрощения (осознанно не реализовано)

- `Application` — record из условия; в реальном проекте JPA-сущность была бы 
  отдельным mutable-классом (records не годятся для Hibernate), поэтому 
  "мутация" в коде — заглушка вместо `withStatus(...)`-копирования.
- Нет различения transient/terminal ошибок лендера (нет exception-иерархии 
  в условии) — сейчас любой сбой = success=false без изменения статуса.
- Optimistic locking (`@Version`) не применим к record напрямую — 
  при необходимости решается через conditional update (`WHERE status = ?`).
