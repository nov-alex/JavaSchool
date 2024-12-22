package sbp.school.kafka.exception;

public class ApplicationConsumerException extends RuntimeException {

    public ApplicationConsumerException(Exception exception) {
        super(exception);
    }

    public ApplicationConsumerException(String message) {
        super(message);
    }
}
