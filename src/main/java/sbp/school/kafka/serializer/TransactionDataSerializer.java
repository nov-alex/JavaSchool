package sbp.school.kafka.serializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sbp.school.kafka.entity.TransactionData;
import sbp.school.kafka.exception.ApplicationSerializationException;
import sbp.school.kafka.util.json.ObjectMapperJson;

import java.nio.charset.StandardCharsets;

/**
 * Сериализатор данных для продюсера кафки
 */

public class TransactionDataSerializer implements Serializer<TransactionData> {

    private static final Logger logger = LoggerFactory.getLogger(TransactionDataSerializer.class);

    /**
     * @param topic           топик кафки ассоциированный с данными
     * @param transactionData Объект для сериализации
     * @return массив данных
     */
    @Override
    public byte[] serialize(String topic, TransactionData transactionData) {
        if (transactionData != null) {
            try {
                ObjectMapper objectMapper = ObjectMapperJson.getObjectMapper();
                return objectMapper
                        .writeValueAsString(transactionData)
                        .getBytes(StandardCharsets.UTF_8);
            } catch (JsonProcessingException e) {
                logger.error("Ошибка сериализатора кафки: {}", e.getMessage());
                throw new ApplicationSerializationException(e);
            }
        }
        return new byte[0];
    }
}
