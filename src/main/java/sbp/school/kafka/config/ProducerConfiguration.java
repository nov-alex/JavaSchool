package sbp.school.kafka.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sbp.school.kafka.exception.ApplicationProducerException;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * Загрузка конфигурации из property-файла
 */
public final class ProducerConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(ProducerConfiguration.class);

    private static final String PRODUCER_CONFIG_FILE = "producer.properties";
    private static final String PRODUCER_TOPIC_CONFIG_FILE = "producer-topic.properties";

    private static Properties producerProperties;
    private static Properties producerTopicProperties;

    private ProducerConfiguration() {
        //NO_OP
    }

    public static Properties getProducerConfig() throws ApplicationProducerException {
        return (producerProperties = loadProperty(PRODUCER_CONFIG_FILE, producerProperties));
    }

    public static Properties getProducerTopicConfig() throws ApplicationProducerException {
        return (producerTopicProperties = loadProperty(PRODUCER_TOPIC_CONFIG_FILE, producerTopicProperties));
    }

    private static Properties loadProperty(String filename, Properties properties) throws ApplicationProducerException {
        if (properties == null) {
            try (InputStream inputStream = ProducerConfiguration.class.getClassLoader().getResourceAsStream(filename)) {
                if (inputStream == null) {
                    logger.error("Не удается найти файл {}", filename);
                    throw new ApplicationProducerException("Не удается найти файл " + filename);
                }
                Properties loadedProperties = new Properties();
                InputStreamReader fileReader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
                loadedProperties.load(fileReader);
                return loadedProperties;
            } catch (IOException e) {
                logger.error(e.getMessage());
                throw new ApplicationProducerException(e);
            }
        }
        return properties;
    }
}
