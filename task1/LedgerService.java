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

@Service
public class LedgerService {
    
    private final LenderClient lenderClient;
    private final ApplicationRepository applicationRepository;
    private final OutboxMessageRepository outboxMessageRepository;
    private final TransactionTemplate txTemplate;

    @Autowired
    public LedgerService(LenderClient lenderClient, ApplicationRepository applicationRepository, 
            OutboxMessageRepository outboxMessageRepository, TransactionTemplate txTemplate) {
        this.lenderClient = lenderClient;
        this.applicationRepository = applicationRepository;
        this.outboxMessageRepository = outboxMessageRepository;
        this.txTemplate = txTemplate;
    }

    public BlockLimitResult blockLenderLimit(UUID id) {

        Application application = applicationRepository.findById(id).orElseThrow(() -> new ApplicationNotFoundException(id));

        BlockLimitResult blockLimitResult = new BlockLimitResult(id);

        // already blocked, so just returning true
        if (application.status() == LIMIT_BLOCKED) {
            blockLimitResult.setSuccess(true);
            return blockLimitResult;
        }

        if (application.status() != SCORING_APPROVED) { // if current status is different from SCORING_APPROVED, proceeding is prohibited 
            blockLimitResult.setSuccess(false);
            // maybe adding some additional data for the result, e.g. setting cause of failure
            return blockLimitResult;
        }

        String requestId = generateRequestId();

        LenderBlockResult clientResult;
        
        try {
            clientResult = lenderClient.blockLimit(application.lenderId(), id, application.requestedAmount(), requestId);
        } catch (Exception ex) { // catching basic Exception since no specific Exception types given
            blockLimitResult.setSuccess(false);
            // maybe adding some additional data for the result, like setting cause of failure
            return blockLimitResult;
        }

        // using transaction in order to guarantee atomicity between saving application and outbox message
        txTemplate.execute(status -> {
            blockLimitResult.setSuccess(true);
            application.setLenderBlockId(clientResult.blockId());
            application.setStatus(LIMIT_BLOCKED); // or creating and saving new Application instance if we want to stick to the template with record (immutable fields)
            applicationRepository.save(application);

            ApplicationLimitBlockedEvent blockedEvent = new ApplicationLimitBlockedEvent(blockLimitResult.getApplicationId()); 

            OutboxMessage outboxMessage = new OutboxMessage(blockedEvent.getApplicationId(), Instant.now(), blockedEvent.toJson(), OutboxMessageStatus.NEW);
            outboxMessageRepository.save(outboxMessage);
            return null;
        });

        return blockLimitResult;
    }

    private String generateRequestId() {
        // some ID generation logic which is determined by application id
    }

}

public class BlockLimitResult { // simplified class for the result of LedgerService.blockLenderLimit(id) method

    private UUID applicationId; 
    private boolean success;

    public BlockLimitResult(UUID applicationId) {
        this.applicationId = applicationId;
    }
    
    // other fields, constructors and methods

} 

public class ApplicationLimitBlockedEvent { // simplified class for the LedgerService.blockLenderLimit(id) success events
    
    private UUID applicationId; 

    public ApplicationLimitBlockedEvent(UUID applicationId) {
        this.applicationId = applicationId;
    }
    
    // other fields, constructors and methods
}

@Slf4j
@Service
public class OutboxMessageProducer {
    
    private final OutboxMessageRepository outboxMessageRepository;
    private final KafkaProducer kafkaProducer;
    private final MessageService messageService;

    @Autowired
    public OutboxMessageProducer(OutboxMessageRepository outboxMessageRepository, KafkaProducer kafkaProducer, MessageService messageService) {
        this.outboxMessageRepository = outboxMessageRepository;
        this.kafkaProducer = kafkaProducer;
        this.messageService = messageService;
    }

    // implementing basing polling every 5 seconds logic, but could use advanced techniques like Debezium
    @Scheduled(fixedDelay = 5000)
    public void pollAndProduce() {

        List<OutboxMessage> messages = outboxMessageRepository.findTop100ByStatusInOrderByTimestampAsc(List.of(OutboxMessageStatus.NEW, OutboxMessageStatus.PENDING));
        
        for (OutboxMessage message : messages) {
            try {
                message.setStatus(OutboxMessageStatus.PENDING);
                outboxMessageRepository.save(message);
                messageService.sendMessage(message);
            } catch (Exception exception) { // catching basic Exception but better to use some more specific type corresponding to fauilure after all retries
                log.error("Unable to send message: {}", message.getId());
                message.setStatus(OutboxMessageStatus.FAILED);
                outboxMessageRepository.save(message);
            }
        }

    }

    // deleting old sent messages
    @Scheduled(cron = "0 0 3 * * *") 
    public void cleanOldMessages() {
        Instant cutoff = Instant.now().minus(30, ChronoUnit.DAYS);
        outboxMessageRepository.deleteByStatusAndTimestampBefore(SENT, cutoff);
    }
}

@Service
public class MessageService {

    private final OutboxMessageRepository outboxMessageRepository;
    private final KafkaProducer kafkaProducer;

    @Autowired
    public MessageService(OutboxMessageRepository outboxMessageRepository, KafkaProducer kafkaProducer) {
        this.outboxMessageRepository = outboxMessageRepository;
        this.kafkaProducer = kafkaProducer;
    }

    // basic Spring retry but could use DLQ too
    @Retryable(
        value = {Exception.class},
        maxAttempts = 3,
        backoff = @Backoff(delay = 1000, multiplier = 2)
    )
    public void sendMessage(OutboxMessage message) {
        kafkaProducer.send("limit-block", message.getAggregateId(), message.getPayload());
        message.setStatus(OutboxMessageStatus.SENT);
        outboxMessageRepository.save(message);
    }
}

public interface OutboxMessageRepository extends JpaRepository<OutboxMessage, Long> {
    // some custom methods
}

@Entity
public class OutboxMessage {
    @Id
    private Long id;

    private UUID aggregateId;

    private Instant timestamp;

    private String payload;

    private OutboxMessageStatus status;

    // other fields, constructors and methods
}

public enum OutboxMessageStatus {
    NEW, PENDING, SENT, FAILED
}

interface KafkaProducer {
    void send(String topic, String key, String payloadJson);
}

interface ApplicationService {

}

interface ApplicationController {

}

