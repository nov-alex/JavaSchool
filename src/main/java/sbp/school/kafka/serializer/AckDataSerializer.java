package sbp.school.kafka.serializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sbp.school.kafka.entity.AckData;
import sbp.school.kafka.exception.ApplicationSerializationException;
import sbp.school.kafka.util.json.ObjectMapperJson;

import java.nio.charset.StandardCharsets;

public class AckDataSerializer implements Serializer<AckData> {

    private static final Logger logger = LoggerFactory.getLogger(AckDataSerializer.class);

    /**
     * @param topic topic associated with data
     * @param data  typed data
     * @return
     */
    @Override
    public byte[] serialize(String topic, AckData data) {
        if (data != null) {
            try {
                ObjectMapper objectMapper = ObjectMapperJson.getObjectMapper();
                return objectMapper
                        .writeValueAsString(data)
                        .getBytes(StandardCharsets.UTF_8);
            } catch (JsonProcessingException e) {
                logger.error("Ошибка сериализатора кафки: {}", e.getMessage());
                throw new ApplicationSerializationException(e);
            }
        }
        return new byte[0];

    }
}
