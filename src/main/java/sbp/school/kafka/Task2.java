package sbp.school.kafka;

import sbp.school.kafka.config.ConsumerConfiguration;
import sbp.school.kafka.service.ConsumerService;
import sbp.school.kafka.storage.ExternalStorage;

import java.util.Arrays;
import java.util.stream.Collectors;

public class Task2 {

    public static void main(String[] args) {

        try {
            String topic = ConsumerConfiguration.getConsumerTopicConfig().getProperty("topic");
            String topicRegExp = ConsumerConfiguration.getConsumerTopicConfig().getProperty("topic.subscribe.regexp");
            ExternalStorage storage = new ExternalStorage();
            ConsumerService service = new ConsumerService(topic, topicRegExp, storage);
            service.read(ConsumerConfiguration.getConsumerConfig());
        } catch (Exception e) {
            System.out.println(String.format("Exception %s:\n%s", e.getClass().getCanonicalName(), e.getMessage()));
            System.out.println(Arrays
                    .stream(e.getStackTrace())
                    .map(s -> s + "\n")
                    .collect(Collectors.joining()));
        }
    }
}
