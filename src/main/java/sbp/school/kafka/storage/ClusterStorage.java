package sbp.school.kafka.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sbp.school.kafka.storage.dto.OffsetDataStorage;
import sbp.school.kafka.storage.dto.TopicDataStorage;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Хранилище для обеспечения работоспоспобности Кафки
 */
public class ClusterStorage {

    private final Logger logger = LoggerFactory.getLogger(ClusterStorage.class);

    private final Map<String, TopicDataStorage> storage = new ConcurrentHashMap<>();

    public void storeOffset(String topic, int partition, long offset) {

        if (storage.containsKey(topic)) {
            TopicDataStorage topicDataStorage = storage.get(topic);
            OffsetDataStorage offsetDataStorage = topicDataStorage.get(partition);
            offsetDataStorage.setUncommitedOffset(offset);
        } else {
            TopicDataStorage topicDataStorage = TopicDataStorage.create(partition, 0L, offset);
            storage.put(topic, topicDataStorage);
        }
    }

    public void commitOffset(String topic, int partition, long offset) {
        if (!storage.containsKey(topic)) {
            storage.put(topic, TopicDataStorage.create(partition, offset, offset));
        } else {
            OffsetDataStorage offsetDataStorage = storage.get(topic).get(partition);
            offsetDataStorage.setUncommitedOffset(offset);
            offsetDataStorage.commit();
        }
    }

    public void commitOffset(String topic, int partition) {
        storage.computeIfAbsent(topic, s ->
                storage.put(topic, TopicDataStorage.create(partition, 0L, 0))
        );
        OffsetDataStorage offsetDataStorage = storage.get(topic).get(partition);
        offsetDataStorage.commit();
    }

    public long getCommitedOffset(String topic, int partition) {
        long offset = 0;
        if (!storage.containsKey(topic)) {
            TopicDataStorage topicDataStorage = TopicDataStorage.create(partition, 0L, 0);
            storage.put(topic, topicDataStorage);
        } else {
            offset = storage.get(topic).get(partition).getCommitedOffset();
        }
        logger.debug("getCommitedOffset: topic {}, offset {}", topic, offset);
        return offset;
    }
}
