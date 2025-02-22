package sbp.school.kafka.service;

import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.concurrent.Future;

/**
 * Интерфейс для потребителя Кафки
 *
 * @param <T> передаваемый тип данных
 */
public interface ProducerService<T> {
    Future<RecordMetadata> send(T t);

    Future<RecordMetadata> resend(T t);
}
