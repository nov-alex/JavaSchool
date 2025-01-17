package sbp.school.kafka.exception;

import org.apache.kafka.common.errors.SerializationException;

public class ApplicationKeyDeserializerException extends SerializationException {

    public ApplicationKeyDeserializerException(String message) {
        super(message);
    }

}
