package sbp.school.kafka.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sbp.school.kafka.exception.ApplicationConsumerException;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class Configuration {

    private static final Logger logger = LoggerFactory.getLogger(Configuration.class);

    private static Properties loadProperty(String filename) throws ApplicationConsumerException {
        try (InputStream inputStream = Configuration.class.getClassLoader().getResourceAsStream(filename)) {
            if (inputStream == null) {
                logger.error("Не удается найти файл {}", filename);
                throw new ApplicationConsumerException("Не удается найти файл " + filename);
            }
            InputStreamReader fileReader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
            Properties properties = new Properties();
            properties.load(fileReader);
            if (properties.isEmpty()) {
                logger.error("Файл {} не является файлом настроек!", filename);
                throw new ApplicationConsumerException("Файл " + filename + " не является файлом настроек!");
            }
            return properties;
        } catch (IOException e) {
            logger.error(e.getMessage());
            throw new ApplicationConsumerException(e);
        }
    }
}
