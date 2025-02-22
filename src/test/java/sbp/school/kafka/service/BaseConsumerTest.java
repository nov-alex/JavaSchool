package sbp.school.kafka.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import sbp.school.kafka.entity.TransactionData;
import sbp.school.kafka.entity.TransactionType;
import sbp.school.kafka.storage.ClusterStorage;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

class BaseConsumerTest {

    private final String TOPIC = "test";
    private final int PARTITION = 0;
    private final UUID UUID_RANDOM = UUID.randomUUID();
    private final TransactionData DATA = new TransactionData(UUID_RANDOM, TransactionType.TRANSACTION_DEBIT, new BigDecimal(5), LocalDateTime.now(), "40005855488554");
    private ClusterStorage storage;
    private MockConsumer<String, TransactionData> consumer;
    private BaseConsumer<TransactionData> baseConsumer;
    private List<TransactionData> dataConsumer;


    @BeforeEach
    void setup() {
        dataConsumer = new ArrayList<>();
        storage = new ClusterStorage();
        consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        baseConsumer = new BaseConsumer<>("test",
                storage,
                consumer,
                dataConsumer::add);
    }


    @Test
    void startByAssign_test() {

        TopicPartition tp = new TopicPartition(TOPIC, PARTITION);

        HashMap<TopicPartition, Long> startingOffset = new HashMap<>();
        startingOffset.put(tp, 0L);

        consumer.updateBeginningOffsets(startingOffset);

        consumer.schedulePollTask(() -> {
            consumer.assign(List.of(tp));
            consumer.addRecord(new ConsumerRecord<>(TOPIC, PARTITION, 0, UUID_RANDOM.toString(), DATA));
            consumer.schedulePollTask(() -> baseConsumer.stop()); // Exec on 2nd poll
        });

        baseConsumer.read();

        Assertions.assertTrue(consumer.closed());
        Assertions.assertEquals(1, dataConsumer.size());
        Assertions.assertEquals(0L, storage.getCommitedOffset(TOPIC, PARTITION));

    }

    @Test
    void startBySubscription_test() {

        TopicPartition tp = new TopicPartition(TOPIC, PARTITION);

        HashMap<TopicPartition, Long> startingOffset = new HashMap<>();
        startingOffset.put(tp, 0L);

        consumer.updateBeginningOffsets(startingOffset);

        consumer.schedulePollTask(() -> {
            consumer.rebalance(List.of(tp));
            consumer.addRecord(new ConsumerRecord<>(TOPIC, PARTITION, 0, UUID_RANDOM.toString(), DATA));
            consumer.schedulePollTask(() -> baseConsumer.stop()); // Exec on 2nd poll
        });

        baseConsumer.run();

        Assertions.assertTrue(consumer.closed());
        Assertions.assertEquals(1, dataConsumer.size());
        Assertions.assertEquals(0L, storage.getCommitedOffset(TOPIC, PARTITION));
    }
}