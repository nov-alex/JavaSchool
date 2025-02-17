package sbp.school.kafka.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sbp.school.kafka.exception.ApplicationProducerException;
import sbp.school.kafka.exception.ApplicationSerializationException;
import sbp.school.kafka.util.json.JsonValidator;
import sbp.school.kafka.util.json.ObjectMapperJson;

import java.util.UUID;
import java.util.concurrent.Future;
import java.util.function.Consumer;

/**
 * Базовый сервис-производитель для передачи данных
 *
 * @param <T> передаваемый тип данных
 */
public class BaseProducer<T> implements ProducerService<T> {

    private static final Logger logger = LoggerFactory.getLogger(BaseProducer.class);

    private final Producer<String, T> producer;
    private final String topic;
    private final JsonValidator jsonValidator;
    private final Consumer<T> consumer;

    public BaseProducer(Producer<String, T> producer, String topic, JsonValidator jsonValidator, Consumer<T> consumer) {
        this.producer = producer;
        this.topic = topic;
        this.jsonValidator = jsonValidator;
        this.consumer = consumer;
    }

    private Future<RecordMetadata> doSend(T t, boolean withAck) {
        try {
            String json = ObjectMapperJson.getObjectMapper().writeValueAsString(t);
            jsonValidator.validate(json);
            return producer.send(new ProducerRecord<>(topic,
                            UUID.randomUUID().toString(),
                            t),
                    (metadata, exception) -> {
                        if (exception != null) {
                            logger.error("Ошибка доставки в кафку: {} -- Topic {}, Partition {}, Offset {}",
                                    exception.getMessage(),
                                    metadata.topic(),
                                    metadata.partition(),
                                    metadata.offset());
                            throw new ApplicationProducerException(exception);
                        } else {
                            if (consumer != null && withAck) {
                                consumer.accept(t);
                            }

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

    @Override
    public Future<RecordMetadata> send(T t) {
        return doSend(t, true);
    }

    @Override
    public Future<RecordMetadata> resend(T t) {
        return doSend(t, false);
    }

}
