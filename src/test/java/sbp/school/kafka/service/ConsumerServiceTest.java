package sbp.school.kafka.service;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import sbp.school.kafka.config.ConsumerConfiguration;
import sbp.school.kafka.storage.ExternalStorage;

class ConsumerServiceTest {

    @BeforeEach
    void setUp() {
    }

    @AfterEach
    void tearDown() {
    }

    @Test
    void read() {
        String topic = ConsumerConfiguration.getConsumerTopicConfig().getProperty("topic");
        String topicRegExp = ConsumerConfiguration.getConsumerTopicConfig().getProperty("topic.subscribe.regexp");
        ExternalStorage storage = new ExternalStorage();
        ConsumerService service = new ConsumerService(topic, topicRegExp, storage);
        service.read(ConsumerConfiguration.getConsumerConfig());
    }
}