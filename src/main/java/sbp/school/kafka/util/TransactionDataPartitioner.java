package sbp.school.kafka.util;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sbp.school.kafka.entity.TransactionData;
import sbp.school.kafka.entity.TransactionType;
import sbp.school.kafka.exception.ApplicationProducerException;

import java.util.List;
import java.util.Map;

public class TransactionDataPartitioner implements Partitioner {

    private static final Logger logger = LoggerFactory.getLogger(TransactionDataPartitioner.class);

    /**
     * Compute the partition for the given record.
     *
     * @param topic      The topic name
     * @param key        The key to partition on (or null if no key)
     * @param keyBytes   The serialized key to partition on( or null if no key)
     * @param value      The value to partition on or null
     * @param valueBytes The serialized value to partition on or null
     * @param cluster    The current cluster metadata
     */
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        if (valueBytes == null
                || !(value instanceof TransactionData)) {
            logger.error("В качестве значения только TransactionData");
            throw new ApplicationProducerException("В качестве ключа должно быть только TransactionData");
        }
        List<PartitionInfo> partitionInfos = cluster.partitionsForTopic(topic);
        int size = partitionInfos.size();
        int expectedPartitionsCount = TransactionType.values().length;
        if (expectedPartitionsCount > size) {
            logger.error("Недостаточное кол-во партиций {}, требуется {}", size, expectedPartitionsCount);
            throw new ApplicationProducerException(String.format("Недостаточное кол-во партиций %d, требуется %d", size, expectedPartitionsCount));
        }

        return ((TransactionData) value).transactionType().ordinal();
    }

    /**
     * This is called when partitioner is closed.
     */
    @Override
    public void close() {

    }

    /**
     * Configure this class with the given key-value pairs
     *
     */
    @Override
    public void configure(Map<String, ?> configs) {

    }
}
