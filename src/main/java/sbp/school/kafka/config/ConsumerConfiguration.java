package sbp.school.kafka.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sbp.school.kafka.exception.ApplicationConsumerException;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * Загрузка конфигурации из property-файла
 */
public final class ConsumerConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerConfiguration.class);

    private static final String CONSUMER_CONFIG_FILE = "consumer.properties";
    private static final String CONSUMER_TOPIC_CONFIG_FILE = "consumer-topic.properties";

    private static final Properties consumerProperties = new Properties();
    private static final Properties consumerTopicProperties = new Properties();

    private ConsumerConfiguration() {
        //NO_OP
    }

    public static Properties getConsumerConfig() throws ApplicationConsumerException {
        if (consumerProperties.isEmpty()) {
            loadProperty(CONSUMER_CONFIG_FILE, consumerProperties);
        }
        return consumerProperties;
    }

    public static Properties getConsumerTopicConfig() throws ApplicationConsumerException {
        if (consumerTopicProperties.isEmpty()) {
            loadProperty(CONSUMER_TOPIC_CONFIG_FILE, consumerTopicProperties);
        }
        return consumerTopicProperties;
    }

    private static void loadProperty(String filename, Properties properties) throws ApplicationConsumerException {
        try (InputStream inputStream = ConsumerConfiguration.class.getClassLoader().getResourceAsStream(filename)) {
            if (inputStream == null) {
                logger.error("Не удается найти файл {}", filename);
                throw new ApplicationConsumerException("Не удается найти файл " + filename);
            }
            InputStreamReader fileReader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
            properties.load(fileReader);
            if (properties.isEmpty()) {
                logger.error("Файл {} не является файлом настроек!", filename);
                throw new ApplicationConsumerException("Файл " + filename + " не является файлом настроек!");
            }
        } catch (IOException e) {
            logger.error(e.getMessage());
            throw new ApplicationConsumerException(e);
        }
    }
}
