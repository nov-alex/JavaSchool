package sbp.school.kafka.exception;

public class ApplicationJsonValidationException extends RuntimeException {

    public ApplicationJsonValidationException(Exception exception) {
        super(exception);
    }

    public ApplicationJsonValidationException(String message) {
        super(message);
    }
}
