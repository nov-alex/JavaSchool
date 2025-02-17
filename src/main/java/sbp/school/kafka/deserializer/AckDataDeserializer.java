package sbp.school.kafka.deserializer;

import com.fasterxml.jackson.core.JacksonException;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sbp.school.kafka.entity.AckData;
import sbp.school.kafka.exception.ApplicationValueDeserializerException;
import sbp.school.kafka.util.json.AckDataJsonValidator;
import sbp.school.kafka.util.json.ObjectMapperJson;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class AckDataDeserializer implements Deserializer<AckData> {


    private static final Logger logger = LoggerFactory.getLogger(AckDataDeserializer.class);

    /**
     * Deserialize a record value from a byte array into a value or object.
     *
     * @param topic topic associated with the data
     * @param data  serialized bytes; may be null; implementations are recommended to handle null by returning a value or null rather than throwing an exception.
     * @return deserialized typed data; may be null
     */
    @Override
    public AckData deserialize(String topic, byte[] data) {
        if (data == null) {
            logger.error("value is null");
            throw new ApplicationValueDeserializerException("value is null");
        }
        try {
            String contentAsString = new String(data, StandardCharsets.UTF_8);

            AckDataJsonValidator.validate(contentAsString);

            return ObjectMapperJson
                    .getObjectMapper()
                    .readValue(contentAsString, AckData.class);

        } catch (ApplicationValueDeserializerException e) {
            logger.error("Ошибка валидации десериализованных данных {}, topic {}, value [{}]", e.getMessage(), topic, data);
            throw new ApplicationValueDeserializerException(String.format("Ошибка валидации десериализованных данных %s, topic %s, value [%s]", e.getMessage(), topic, Arrays.toString(data)));
        } catch (JacksonException e) {
            logger.error("Ошибка преобразования в AckData: {}, topic {}", e.getMessage(), topic);
            throw new ApplicationValueDeserializerException(String.format("Ошибка преобразования в TransactionData: %s, topic %s", e.getMessage(), topic));
        }
    }
}
