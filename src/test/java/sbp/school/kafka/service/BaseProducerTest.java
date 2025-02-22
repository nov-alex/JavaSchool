package sbp.school.kafka.service;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import sbp.school.kafka.entity.TransactionData;
import sbp.school.kafka.entity.TransactionType;
import sbp.school.kafka.exception.ApplicationValueDeserializerException;
import sbp.school.kafka.serializer.TransactionDataSerializer;
import sbp.school.kafka.util.json.TransactionDataJsonValidator;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

class BaseProducerTest {

    private final String TOPIC = "test";
    private final UUID UUID_RANDOM = UUID.randomUUID();
    private final TransactionData DATA = new TransactionData(UUID_RANDOM, TransactionType.TRANSACTION_DEBIT, new BigDecimal(5), LocalDateTime.now(), "47084540005855488554");
    private List<TransactionData> dataConsumer;
    private MockProducer<String, TransactionData> producer;
    private BaseProducer<TransactionData> baseProducer;

    @BeforeEach
    void setup() {
        producer = new MockProducer<>(true, new StringSerializer(), new TransactionDataSerializer());
        dataConsumer = new ArrayList<>();
    }

    @Test
    void send_test() {
        baseProducer = new BaseProducer<>(producer,
                TOPIC,
                TransactionDataJsonValidator.VALIDATOR,
                dataConsumer::add);

        baseProducer.send(DATA);

        Assertions.assertEquals(1, producer.history().size());
        Assertions.assertEquals(1, dataConsumer.size());
        org.assertj.core.api.Assertions.assertThat(DATA)
                .as("Переданный и полученный объект не совпадает!")
                .isEqualTo(producer.history().get(0).value());
    }

    @Test
    void resend_test() {
        baseProducer = new BaseProducer<>(producer,
                TOPIC,
                TransactionDataJsonValidator.VALIDATOR,
                null);

        baseProducer.send(DATA);

        Assertions.assertEquals(1, producer.history().size());
        org.assertj.core.api.Assertions.assertThat(DATA)
                .as("Переданный и полученный объект не совпадает!")
                .isEqualTo(producer.history().get(0).value());
    }

    @Test
    void sendWithValidationException() {
        baseProducer = new BaseProducer<>(producer,
                TOPIC,
                TransactionDataJsonValidator.VALIDATOR,
                dataConsumer::add);

        Executable executable = () -> baseProducer.send(new TransactionData(UUID_RANDOM, TransactionType.TRANSACTION_DEBIT, new BigDecimal(5), LocalDateTime.now(), "470845400058554884"));

        Assertions.assertThrows(ApplicationValueDeserializerException.class, executable);
        Assertions.assertTrue(dataConsumer.isEmpty());
    }

}