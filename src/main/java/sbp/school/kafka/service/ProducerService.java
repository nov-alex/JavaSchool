package sbp.school.kafka.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sbp.school.kafka.entity.TransactionData;
import sbp.school.kafka.exception.ApplicationProducerException;
import sbp.school.kafka.exception.ApplicationSerializationException;
import sbp.school.kafka.util.json.ObjectMapperJson;
import sbp.school.kafka.util.json.TransactionDataJsonValidator;

import java.util.UUID;

public class ProducerService {

    private static final Logger logger = LoggerFactory.getLogger(ProducerService.class);

    private final KafkaProducer<String, TransactionData> producer;
    private final String topic;
    private final TransactionDataJsonValidator transactionDataJsonValidator;

    public ProducerService(String topic, KafkaProducer<String, TransactionData> producer, TransactionDataJsonValidator transactionDataJsonValidator) {
        this.producer = producer;
        this.topic = topic;
        this.transactionDataJsonValidator = transactionDataJsonValidator;
    }

    public void send(TransactionData transactionData) {
        try {
            String json = ObjectMapperJson.getObjectMapper().writeValueAsString(transactionData);
            transactionDataJsonValidator.validate(json);
            producer.send(new ProducerRecord<>(topic,
                            UUID.randomUUID().toString(),
                            transactionData),
                    (metadata, exception) -> {
                        if (exception != null) {
                            logger.error("Ошибка доставки в кафку: {} -- Topic {}, Partition {}, Offset {}",
                                    exception.getMessage(),
                                    metadata.topic(),
                                    metadata.partition(),
                                    metadata.offset());
                            throw new ApplicationProducerException(exception);
                        } else {
                            logger.trace("Успешная доставка в кафку: Topic {}, Partition {}, Offset {}",
                                    metadata.topic(),
                                    metadata.partition(),
                                    metadata.offset());
                        }
                    });
        } catch (JsonProcessingException e) {
            logger.error("Ошибка отправки в кафку {}", e.getMessage());
            throw new ApplicationSerializationException(e);
        }
    }
}
