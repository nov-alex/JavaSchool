package sbp.school.kafka.service;

import com.networknt.schema.JsonSchema;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import sbp.school.kafka.config.ProducerConfiguration;
import sbp.school.kafka.entity.TransactionData;
import sbp.school.kafka.entity.TransactionType;
import sbp.school.kafka.util.json.TransactionDataJsonValidator;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

class ProducerServiceTest {

    private ProducerService service;
    KafkaProducer<String, TransactionData> producer;

    @BeforeEach
    void setUp() {

        String topic = ProducerConfiguration.getProducerTopicConfig().getProperty("topic");
        JsonSchema jsonSchema = TransactionDataJsonValidator.readJsonSchema();
        TransactionDataJsonValidator jsonValidator = new TransactionDataJsonValidator(jsonSchema);
        producer = new KafkaProducer<>(ProducerConfiguration.getProducerConfig());
        service = new ProducerService(topic,
                producer,
                jsonValidator);
    }

    @AfterEach
    void tearDown() {
        producer.close();
    }

    @Test
    void send() {
        List<TransactionData> data = generateTransactionData();
        assertDoesNotThrow(() -> data.forEach(service::send));
        producer.flush();
    }

    /**
     * Данные для отправки в кафку
     *
     * @return список объектов для отправки
     */

    private static List<TransactionData> generateTransactionData() {
        Supplier<String> accountNumberGenerator = () -> {
            StringBuilder str = new StringBuilder();
            Random random = new Random();
            for (int i = 0; i < 20; i++) {
                str.append(random.nextInt(10));
            }
            return str.toString();
        };

        List<TransactionData> data = new ArrayList<>();
        data.add(new TransactionData(UUID.randomUUID(),
                TransactionType.TRANSACTION_CREDIT,
                new BigDecimal("50.01"),
                LocalDate.now(),
                accountNumberGenerator.get()));

        data.add(new TransactionData(UUID.randomUUID(),
                TransactionType.TRANSACTION_DEBIT,
                new BigDecimal("50.02"),
                LocalDate.now(),
                accountNumberGenerator.get()));

        data.add(new TransactionData(UUID.randomUUID(),
                TransactionType.TRANSACTION_CHARGEBACK,
                new BigDecimal("50.03"),
                LocalDate.now(),
                accountNumberGenerator.get()));

        return data;
    }
}