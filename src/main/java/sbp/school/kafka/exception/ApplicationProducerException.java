package sbp.school.kafka.exception;

public class ApplicationProducerException extends RuntimeException {

    public ApplicationProducerException(Exception exception) {
        super(exception);
    }

    public ApplicationProducerException(String message) {
        super(message);
    }
}
