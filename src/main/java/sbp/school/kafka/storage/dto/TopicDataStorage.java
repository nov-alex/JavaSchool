package sbp.school.kafka.storage.dto;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Хранилище для данных топиков
 */
public class TopicDataStorage {

    private final Map<Integer, OffsetDataStorage> storagePartition = new ConcurrentHashMap<>();

    public static TopicDataStorage create(int partition, long commitedOffset, long offset) {
        OffsetDataStorage storage = new OffsetDataStorage(commitedOffset, offset);
        TopicDataStorage topicDataStorage = new TopicDataStorage();
        topicDataStorage.put(partition, storage);
        return topicDataStorage;
    }

    public void put(int partition, OffsetDataStorage storage) {
        storagePartition.put(partition, storage);
    }

    public OffsetDataStorage get(int partition) {
        if (!storagePartition.containsKey(partition)) {
            storagePartition.put(partition, new OffsetDataStorage(0, 0));
        }
        return storagePartition.get(partition);
    }

    public void commit() {
        storagePartition.forEach((s, offsetDataStorage) -> offsetDataStorage.commit());
    }
}
