package sbp.school.kafka.service;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sbp.school.kafka.storage.ExternalStorage;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/**
 *
 */
public class ConsumerService {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerService.class);

    private final String topic;
    private final String topicRegExp;
    private final ExternalStorage storage;
    private final Map<TopicPartition, OffsetAndMetadata> currentOffsets;

    public ConsumerService(String topic, String topicRegExp, ExternalStorage storage) {
        this.topic = topic;
        this.topicRegExp = topicRegExp;
        this.storage = storage;
        this.currentOffsets = new ConcurrentHashMap<>();
    }

    public void read(Properties properties) {

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
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
                        consumer.seek(partition, storage.getCommitedOffset(partition.topic(), partition.partition()));// 2
                    }
                }
            });

            while (true) {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100L));
                if (consumerRecords.isEmpty()) {
                    continue;
                }
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
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

                    currentOffsets.put(new TopicPartition(consumerRecord.topic(), consumerRecord.partition()),
                            new OffsetAndMetadata(consumerRecord.offset() + 1, "no metadata"));
                }
                consumer.commitAsync(currentOffsets,
                        (offsets, exception) -> {
                            for (Map.Entry<TopicPartition, OffsetAndMetadata> offset : offsets.entrySet()) {
                                logger.info("{}: topic = {}, partition = {}, offset = {}",
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
                        });
            }
        }
    }
}