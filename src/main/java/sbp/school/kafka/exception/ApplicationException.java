package sbp.school.kafka.exception;

public class ApplicationException extends RuntimeException {

    public ApplicationException(String message) {
        super(message);
    }

    public ApplicationException(Exception exception) {
        super(exception);
    }
}
