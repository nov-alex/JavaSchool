package sbp.school.kafka.partitioner;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import sbp.school.kafka.entity.TransactionData;
import sbp.school.kafka.entity.TransactionType;
import sbp.school.kafka.exception.ApplicationProducerException;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static java.util.Collections.emptySet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TransactionDataPartitionerTest {

    private final String TOPIC = "test";
    private final UUID UUID_RANDOM = UUID.randomUUID();

    private Cluster cluster;
    private TransactionDataPartitioner partitioner;
    PartitionInfo partitionInfo0 = new PartitionInfo(TOPIC, 0, null, null, null);
    PartitionInfo partitionInfo1 = new PartitionInfo(TOPIC, 1, null, null, null);
    PartitionInfo partitionInfo3 = new PartitionInfo(TOPIC, 3, null, null, null);

    @BeforeEach
    void setup() {
        List<PartitionInfo> partitonList = new ArrayList<>();

        partitonList.add(partitionInfo0);
        partitonList.add(partitionInfo1);
        partitonList.add(partitionInfo3);

        cluster = new Cluster("clusterId", new ArrayList<>(), partitonList, emptySet(), emptySet());

        partitioner = new TransactionDataPartitioner();
    }

    @Test
    void selectPartition0_test() {
        TransactionData data = new TransactionData(UUID_RANDOM, TransactionType.TRANSACTION_DEBIT, new BigDecimal(5), LocalDateTime.now().minusSeconds(55L), "47084540005855488554");

        int result = partitioner.partition(TOPIC,
                null,
                null,
                data,
                null,
                cluster);

        assertEquals(0, result);
    }

    @Test
    void selectPartition1_test() {
        TransactionData data = new TransactionData(UUID_RANDOM, TransactionType.TRANSACTION_CREDIT, new BigDecimal(5), LocalDateTime.now().minusSeconds(55L), "47084540005855488554");

        int result = partitioner.partition(TOPIC,
                null,
                null,
                data,
                null,
                cluster);

        assertEquals(1, result);
    }

    @Test
    void selectPartition2_test() {
        TransactionData data = new TransactionData(UUID_RANDOM, TransactionType.TRANSACTION_CHARGEBACK, new BigDecimal(5), LocalDateTime.now().minusSeconds(55L), "47084540005855488554");

        int result = partitioner.partition(TOPIC,
                null,
                null,
                data,
                null,
                cluster);

        assertEquals(2, result);
    }

    @Test
    void sendInvalidObject_test() {
        Object data = new Object();

        assertThrows(ApplicationProducerException.class,
                () -> partitioner.partition(TOPIC,
                        null,
                        null,
                        data,
                        null,
                        cluster));
    }
}