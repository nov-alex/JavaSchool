package sbp.school.kafka.deserializer;

import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

/**
 *  Десериалайзер для ключа
 */
public class KeyDeserializer implements Deserializer<String> {

    private static final Logger logger = LoggerFactory.getLogger(KeyDeserializer.class);

    /**
     * Deserialize a record value from a byte array into a value or object.
     *
     * @param topic topic associated with the data
     * @param data  serialized bytes; may be null; implementations are recommended to handle null by returning a value or null rather than throwing an exception.
     * @return deserialized typed data; may be null
     */
    @Override
    public String deserialize(String topic, byte[] data) {
        if (data == null) {
            logger.error("Ключ не может быть null");
            return null;
        }
        return new String(data, StandardCharsets.UTF_8);
    }
}
