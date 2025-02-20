package sbp.school.kafka.service;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import sbp.school.kafka.entity.AckData;
import sbp.school.kafka.entity.TransactionData;
import sbp.school.kafka.entity.TransactionType;
import sbp.school.kafka.serializer.TransactionDataSerializer;
import sbp.school.kafka.storage.AckStorage;
import sbp.school.kafka.storage.dto.AckStorageData;
import sbp.school.kafka.storage.enums.AckStatus;
import sbp.school.kafka.util.ack.HashUtils;
import sbp.school.kafka.util.json.TransactionDataJsonValidator;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

class AckConsumerServiceTest {

    private final String TOPIC = "test";
    private final UUID UUID_RANDOM = UUID.randomUUID();
    private final TransactionData DATA = new TransactionData(UUID_RANDOM, TransactionType.TRANSACTION_DEBIT, new BigDecimal(5), LocalDateTime.now(), "47084540005855488554");
    private final AckStorageData ackStorageData = new AckStorageData().setData(DATA).setAckStatus(AckStatus.ACK_PENDING);
    private final AckData ACK_DATA = new AckData(DATA.date().plusMinutes(1), DATA.date().minusMinutes(1), HashUtils.calculateHash(List.of(ackStorageData)));

    private MockProducer<String, TransactionData> producer;

    private BaseProducer<TransactionData> baseProducer;

    private AckConsumerService ackConsumerService;

    private AckStorage ackStorage;
    private List<TransactionData> dataConsumer;

    @BeforeEach
    void setup() {
        ackStorage = new AckStorage();
        producer = new MockProducer<>(true, new StringSerializer(), new TransactionDataSerializer());
        dataConsumer = new ArrayList<>();
    }


    @Test
    void AckValidThenEnd() {

        baseProducer = new BaseProducer<>(producer,
                TOPIC,
                TransactionDataJsonValidator.VALIDATOR,
                dataConsumer::add);

        ackConsumerService = new AckConsumerService(baseProducer, ackStorage);

        ackStorage.put(DATA);

        ackConsumerService.ackProcessor(ACK_DATA);

        Assertions.assertEquals(0, producer.history().size());
        Assertions.assertTrue(ackStorage.poll().isEmpty());
        Assertions.assertTrue(dataConsumer.isEmpty());

    }

    @Test
    void AckInvalidThenProducerResend() {

        baseProducer = new BaseProducer<>(producer,
                TOPIC,
                TransactionDataJsonValidator.VALIDATOR,
                dataConsumer::add);

        ackConsumerService = new AckConsumerService(baseProducer, ackStorage);

        ackStorage.put(new TransactionData(UUID_RANDOM, TransactionType.TRANSACTION_DEBIT, new BigDecimal(5), LocalDateTime.now(), "47084540005855488554"));

        ackConsumerService.ackProcessor(ACK_DATA);

        Assertions.assertEquals(1, producer.history().size());
        Assertions.assertTrue(ackStorage.poll().isEmpty());
        Assertions.assertTrue(dataConsumer.isEmpty());

    }
}