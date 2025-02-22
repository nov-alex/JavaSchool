package sbp.school.kafka.service;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sbp.school.kafka.storage.ClusterStorage;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/**
 * Базовый сервис-потребитель для передачи данных
 *
 * @param <T> принимаемый тип данных
 */
public class BaseConsumer<T> implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(BaseConsumer.class);

    private final String topicRegExp;
    private final Map<TopicPartition, OffsetAndMetadata> currentOffsets;
    private ClusterStorage storage;
    private final Consumer<String, T> consumer;
    private final java.util.function.Consumer<T> dataConsumer;

    public BaseConsumer(String topicRegExp, ClusterStorage storage, Consumer<String, T> consumer, java.util.function.Consumer<T> dataConsumer) {
        this.topicRegExp = topicRegExp;
        this.storage = storage;
        this.consumer = consumer;
        this.currentOffsets = new ConcurrentHashMap<>();
        this.dataConsumer = dataConsumer;
    }

    public void read() {

        try {

            ConsumerRecords<String, T> consumerRecords;

            while (true) {
                try {
                    consumerRecords = consumer.poll(Duration.ofMillis(100L));
                    if (consumerRecords.isEmpty()) {
                        continue;
                    }
                    for (ConsumerRecord<String, T> consumerRecord : consumerRecords) {
                        if (consumerRecord.key() == null || consumerRecord.value() == null) {
                            logger.debug("Message skip due key|value=null: topic = {}, partition = {}, offset = {}, key = {}, value = {}\n",
                                    consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset(),
                                    consumerRecord.key(), consumerRecord.value());
                        } else {
                            logger.debug("topic = {}, partition = {}, offset = {}, key = {}, value = {}\n",
                                    consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset(),
                                    consumerRecord.key(), consumerRecord.value());
                            storage.storeOffset(consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset());
                        }

                        if (dataConsumer != null) {
                            dataConsumer.accept(consumerRecord.value());
                        }

                        currentOffsets.put(new TopicPartition(consumerRecord.topic(), consumerRecord.partition()),
                                new OffsetAndMetadata(consumerRecord.offset() + 1, "no metadata"));
                    }
                } catch (SerializationException e) {
                    RecordDeserializationException recordDeserializationException = ((RecordDeserializationException) e);
                    logger.error("Consumer Deserialization Exception: {}, topic = {}, partition = {}, offset = {}",
                            e.getMessage(),
                            recordDeserializationException.topicPartition().topic(),
                            recordDeserializationException.topicPartition().partition(),
                            recordDeserializationException.offset());
                    currentOffsets.put(recordDeserializationException.topicPartition(),
                            new OffsetAndMetadata(recordDeserializationException.offset() + 1, "no metadata"));
                }
                consumer.commitAsync(currentOffsets, callback);
            }
        } catch (WakeupException exception) {
            logger.debug("Shutting down consumer...");
        } finally {
            consumer.close();
        }
    }

    public void stop() {
        consumer.wakeup();
    }

    private final OffsetCommitCallback callback = (offsets, exception) -> {
        for (Map.Entry<TopicPartition, OffsetAndMetadata> offset : offsets.entrySet()) {
            logger.trace("{}: topic = {}, partition = {}, offset = {}",
                    exception == null ? "commit success" : "commit failed",
                    offset.getKey().topic(),
                    offset.getKey().partition(),
                    offset.getValue().offset());

            if (exception == null) {
                storage.storeOffset(offset.getKey().topic(),
                        offset.getKey().partition(),
                        offset.getValue().offset());
            } else {
                storage.commitOffset(offset.getKey().topic(),
                        offset.getKey().partition(),
                        offset.getValue().offset());
            }
        }
    };

    private void subscribe(Consumer<String, T> consumer) {
        consumer.subscribe(Pattern.compile(topicRegExp), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                logger.debug("onPartitionsRevoked: {}", partitions.size());
                for (TopicPartition partition : partitions) {
                    storage.commitOffset(partition.topic(), partition.partition());
                }
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                logger.debug("onPartitionsAssigned: {}", partitions.size());
                for (TopicPartition partition : partitions) {
                    consumer.seek(partition, storage.getCommitedOffset(partition.topic(), partition.partition()));
                }
            }
        });
    }

    @Override
    public void run() {
        subscribe(consumer);
        read();
    }
}
