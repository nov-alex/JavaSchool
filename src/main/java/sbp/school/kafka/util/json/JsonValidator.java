package sbp.school.kafka.util.json;

import sbp.school.kafka.exception.ApplicationValueDeserializerException;

public interface JsonValidator {
    void validate(String jsonData) throws ApplicationValueDeserializerException;
}
