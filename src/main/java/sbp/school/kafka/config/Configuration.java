package sbp.school.kafka.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sbp.school.kafka.exception.ApplicationException;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class Configuration {

    private static final Logger logger = LoggerFactory.getLogger(Configuration.class);

    public static Properties loadProperty(String filename) throws ApplicationException {
        try (InputStream inputStream = Configuration.class.getClassLoader().getResourceAsStream(filename)) {
            if (inputStream == null) {
                logger.error("Не удается найти файл {}", filename);
                throw new ApplicationException("Не удается найти файл " + filename);
            }
            InputStreamReader fileReader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
            Properties properties = new Properties();
            properties.load(fileReader);
            if (properties.isEmpty()) {
                logger.error("Файл {} не является файлом настроек!", filename);
                throw new ApplicationException("Файл " + filename + " не является файлом настроек!");
            }
            return properties;
        } catch (IOException e) {
            logger.error(e.getMessage());
            throw new ApplicationException(e);
        }
    }
}
