package sbp.school.kafka.exception;

import org.apache.kafka.common.errors.SerializationException;

public class ApplicationValueDeserializerException extends SerializationException {

    public ApplicationValueDeserializerException(Exception exception) {
        super(exception);
    }

    public ApplicationValueDeserializerException(String message) {
        super(message);
    }
}
