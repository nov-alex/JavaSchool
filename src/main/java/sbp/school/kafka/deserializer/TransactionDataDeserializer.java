package sbp.school.kafka.deserializer;

import com.fasterxml.jackson.core.JacksonException;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sbp.school.kafka.entity.TransactionData;
import sbp.school.kafka.exception.ApplicationJsonValidationException;
import sbp.school.kafka.util.json.ObjectMapperJson;
import sbp.school.kafka.util.json.TransactionDataJsonValidator;

import java.nio.charset.StandardCharsets;

/**
 *  Десериалайзер для значения
 */
public class TransactionDataDeserializer implements Deserializer<TransactionData> {

    private static final Logger logger = LoggerFactory.getLogger(TransactionDataDeserializer.class);

    /**
     * Deserialize a record value from a byte array into a value or object.
     *
     * @param topic topic associated with the data
     * @param data  serialized bytes; may be null; implementations are recommended to handle null by returning a value or null rather than throwing an exception.
     * @return deserialized typed data; may be null
     */
    @Override
    public TransactionData deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        try {
            String contentAsString = new String(data, StandardCharsets.UTF_8);

            TransactionDataJsonValidator.validate(contentAsString);

            return ObjectMapperJson
                    .getObjectMapper()
                    .readValue(contentAsString, TransactionData.class);

        } catch (ApplicationJsonValidationException e) {
            logger.error("Ошибка валидации десериализованных данных {}, topic {}, value [{}]", e.getMessage(), topic, data);
        } catch (JacksonException e) {
            logger.error("Ошибка преобразования в TransactionData: {}, topic {}", e.getMessage(), topic);
        }
        return null;
    }
}
