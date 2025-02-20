package sbp.school.kafka.service;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import sbp.school.kafka.entity.AckData;
import sbp.school.kafka.entity.TransactionData;
import sbp.school.kafka.entity.TransactionType;
import sbp.school.kafka.serializer.AckDataSerializer;
import sbp.school.kafka.storage.AckStorage;
import sbp.school.kafka.storage.dto.AckConfigDto;
import sbp.school.kafka.util.json.AckDataJsonValidator;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.UUID;

class AckProducerServiceTest {

    private final String TOPIC = "test";
    private final UUID UUID_RANDOM = UUID.randomUUID();
    private final AckConfigDto config = new AckConfigDto(Duration.of(60L, ChronoUnit.SECONDS), Duration.of(60L, ChronoUnit.SECONDS));
    private final TransactionData DATA = new TransactionData(UUID_RANDOM, TransactionType.TRANSACTION_DEBIT, new BigDecimal(5), LocalDateTime.now().minusSeconds(55L), "47084540005855488554");

    private AckProducerService ackProducerService;
    private AckStorage ackStorage;

    private BaseProducer<AckData> baseProducer;
    private MockProducer<String, AckData> producer;


    @BeforeEach
    void setup() {
        ackStorage = new AckStorage();
        producer = new MockProducer<>(true, new StringSerializer(), new AckDataSerializer());
    }

    @Test
    void sendAck() {
        baseProducer = new BaseProducer<>(producer,
                TOPIC,
                AckDataJsonValidator.VALIDATOR,
                null);

        ackProducerService = new AckProducerService(baseProducer, ackStorage, config);
        System.out.println(DATA);
        ackStorage.put(DATA);

        ackProducerService.service();

        Assertions.assertEquals(1, producer.history().size());
        Assertions.assertTrue(ackStorage.poll().isEmpty());

    }
}