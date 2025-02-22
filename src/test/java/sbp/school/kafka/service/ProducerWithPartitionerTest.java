package sbp.school.kafka.service;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import sbp.school.kafka.entity.TransactionData;
import sbp.school.kafka.entity.TransactionType;
import sbp.school.kafka.exception.ApplicationProducerException;
import sbp.school.kafka.serializer.TransactionDataSerializer;
import sbp.school.kafka.partitioner.TransactionDataPartitioner;
import sbp.school.kafka.util.json.TransactionDataJsonValidator;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static java.util.Collections.emptySet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ProducerWithPartitionerTest {

    private final String TOPIC = "test";
    private final UUID UUID_RANDOM = UUID.randomUUID();
    private final TransactionData DATA = new TransactionData(UUID_RANDOM, TransactionType.TRANSACTION_CREDIT, new BigDecimal(5), LocalDateTime.now().minusSeconds(55L), "47084540005855488554");

    private Cluster cluster;
    private BaseProducer<TransactionData> baseProducer;
    private MockProducer<String, TransactionData> producer;
    private List<PartitionInfo> partitonList;
    PartitionInfo partitionInfo0 = new PartitionInfo(TOPIC, 0, null, null, null);
    PartitionInfo partitionInfo1 = new PartitionInfo(TOPIC, 1, null, null, null);
    PartitionInfo partitionInfo3 = new PartitionInfo(TOPIC, 3, null, null, null);

    @BeforeEach
    void setup() {
        partitonList = new ArrayList<>();
    }

    @Test
    void whenSendWithPartitioner_thenVerifyPartitionNumber() throws ExecutionException, InterruptedException {
        partitonList.add(partitionInfo0);
        partitonList.add(partitionInfo1);
        partitonList.add(partitionInfo3);

        cluster = new Cluster("clusterId", new ArrayList<>(), partitonList, emptySet(), emptySet());

        producer = new MockProducer<>(cluster,
                true,
                new TransactionDataPartitioner(),
                new StringSerializer(),
                new TransactionDataSerializer());

        baseProducer = new BaseProducer<>(producer,
                TOPIC,
                TransactionDataJsonValidator.VALIDATOR,
                null);

        Future<RecordMetadata> recordMetadataFuture = baseProducer.send(DATA);

        assertEquals(1, recordMetadataFuture.get().partition());
    }

    @Test
    void whenSendWithPartitioner_withPartitionCountLessExpected_thenThrowException() {
        partitonList.add(partitionInfo0);
        partitonList.add(partitionInfo3);

        cluster = new Cluster("clusterId", new ArrayList<>(), partitonList, emptySet(), emptySet());

        producer = new MockProducer<>(cluster,
                true,
                new TransactionDataPartitioner(),
                new StringSerializer(),
                new TransactionDataSerializer());

        baseProducer = new BaseProducer<>(producer,
                TOPIC,
                TransactionDataJsonValidator.VALIDATOR,
                null);

        assertThrows(ApplicationProducerException.class, () -> baseProducer.send(DATA));

    }

}
