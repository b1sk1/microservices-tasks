package interview.service;

import java.util.UUID;

@Service
@Slf4j
public class ApplicationServiceJava {

    private final ApplicationRepository applicationRepository;
    private final KycService kycService;
    private final NotificationService notificationService;

    public ApplicationServiceJava(ApplicationRepository applicationRepository, KycService kycService, NotificationService notificationService) {
        this.applicationRepository = applicationRepository;
        this.kycService = kycService;
        this.notificationService = notificationService;
    }

    /**
     * Goal: complete KYC so the application moves from
     * KYC_PENDING to KYC_COMPLETED in the database
     * and the applicant is notified
     */
    public Application completeKyc(UUID applicationId, UUID clientId) {
        
        Application application = applicationRepository.findById(applicationId).orElseThrow(() -> new ApplicationNotFoundException(applicationId));

        if (!application.getClientId().equals(clientId)) {
            throw new IllegalArgumentException("Client does not own application");
        }

        if (application.getStatus() != ApplicationStatus.KYC_PENDING) { // shouldn't proceed if the status is incorrect
            UnableToCompleteKycException unableToCompleteKycException = new UnableToCompleteKycException("Incorrect initial status: " + application.getStatus(), application.getkycSessionId());
            throw unableToCompleteKycException;
        } 

        KycResult kycResult;

        try {
            kycResult = kycService.sendKycRequest(application.getkycSessionId());
        } catch (Exception exception) { // catching basic exception but could catch more specific ones e.g. business-logic exceptions or exceptions corresponding to failure of all retries  
            log.error("Unable to complete KYC for kycSessionId={}", application.getkycSessionId());
            UnableToCompleteKycException unableToCompleteKycException = new UnableToCompleteKycException("Client failed to answer after all retries", exception);
            throw unableToCompleteKycException;
        }

        if (kycResult.completed()) {
            application.setStatus(ApplicationStatus.KYC_COMPLETED); // or create new Application object if we want to stick to record instead of class
            applicationRepository.update(application); // could also write native SQL / JPQL query checking that ApplicationStatus still equals to KYC_PENDING in order to prevent concurrency issues
            SmsRequest smsRequest = new SmsRequest(clientId, "The result of your KYC is the following: " + kycResult);
        
            try {
                NotificationResult notificationResult = notificationService.sendNotification(smsRequest); // could also use outbox + transaction to ensure that notification will not be lost

                if (notificationResult instanceof NotificationResult.ValidationError) {
                    log.error("Validation Error for applicationId={}, smsRequest={}", applicationId, smsRequest);
                }
            } catch (Exception exception) { // catching basic exception but could catch more specific ones e.g. business-logic exceptions or exceptions corresponding to failure of all retries  
                log.error("Failed to get notificationResult for applicationId={}", applicationId, exception);
            }

        }
        
        return application;
    }

    public record Application(UUID id, UUID clientId,
            ApplicationStatus status, String kycSessionId) {}

    public enum ApplicationStatus {
        INITIAL,
        KYC_PENDING,
        KYC_COMPLETED,
        APPLICATION_COMPLETED
    }

    public record KycResult(boolean completed, String details) {}

    public record SmsRequest(String clientId, String text) {}

    public sealed interface NotificationResult permits
            NotificationResult.Success, NotificationResult.ValidationError,
            NotificationResult.ValidationError {}
            
    public record ValidationError(String reason) implements
            NotificationResult {}

    public interface ApplicationRepository {
        Application findById(UUID id);
        Application update(Application application);
    }

    public interface NotificationClient {
        NotificationResult sendSms(SmsRequest request);
    }

    public interface KycClient {
        KycResult fetchStatus(String kycSessionId);
    }
}

@Service
public class KycService {

    private final KycClient kycClient;
    
    @Autowired
    public KycService(KycClient kycClient) {
        this.kycClient = kycClient;
    }

    @Retryable(
        value = {Exception.class}, // catching basic exception since no client realization given
        maxAttempts = 3,
        backoff = @Backoff(delay = 1000, multiplier = 2)
    )
    public KycResult sendKycRequest(String kycSessionId) {
        return kycClient.fetchStatus(kycSessionId);
    }
}

@Service
public class NotificationService {

    private final NotificationClient notificationClient;

    @Autowired
    public NotificationService(NotificationClient notificationClient) {
        this.notificationClient = notificationClient;
    }

    @Retryable(
        value = {Exception.class}, // catching basic exception since no client realization given
        maxAttempts = 3,
        backoff = @Backoff(delay = 1000, multiplier = 2)
    )
    public NotificationResult sendNotification(SmsRequest smsRequest) throws InterruptedException {
        return notificationClient.sendSms(smsRequest);
    }
}
