package sbp.school.kafka.exception;

import org.apache.kafka.common.errors.SerializationException;

public class ApplicationSerializationException extends SerializationException {

    public ApplicationSerializationException(Exception exception) {
        super(exception);
    }

    public ApplicationSerializationException(String message) {
        super(message);
    }
}
