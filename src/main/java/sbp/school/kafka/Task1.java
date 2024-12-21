package sbp.school.kafka;

import com.networknt.schema.JsonSchema;
import org.apache.kafka.clients.producer.KafkaProducer;
import sbp.school.kafka.config.ProducerConfiguration;
import sbp.school.kafka.entity.TransactionData;
import sbp.school.kafka.entity.TransactionType;
import sbp.school.kafka.service.ProducerService;
import sbp.school.kafka.util.json.TransactionDataJsonValidator;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class Task1 {
    public static void main(String[] args) {
        try {
            List<TransactionData> data = generateTransactionData();
            String topic = ProducerConfiguration.getProducerTopicConfig().getProperty("topic");
            JsonSchema jsonSchema = TransactionDataJsonValidator.readJsonSchema();
            TransactionDataJsonValidator jsonValidator = new TransactionDataJsonValidator(jsonSchema);
            try (KafkaProducer<String, TransactionData> producer = new KafkaProducer<>(ProducerConfiguration.getProducerConfig())) {
                ProducerService service = new ProducerService(topic,
                        producer,
                        jsonValidator);

                data.forEach(service::send);
                producer.flush();
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
            System.out.println(Arrays
                    .stream(e.getStackTrace())
                    .map(s -> s + "\n")
                    .collect(Collectors.joining()));
        }
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
