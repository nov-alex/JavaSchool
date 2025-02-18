package sbp.school.kafka.sink;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.Map;

import static sbp.school.kafka.sink.FileSinkConnector.CONFIG_DEF;
import static sbp.school.kafka.sink.FileSinkConnector.FILENAME;


public class FileSinkTask extends SinkTask {

    private static final Logger logger = LoggerFactory.getLogger(FileSinkTask.class);

    private String filename;
    private BufferedWriter outputStream;

    @Override
    public void start(Map<String, String> props) {
        AbstractConfig config = new AbstractConfig(CONFIG_DEF, props);
        filename = config.getString(FILENAME);
        try {
            outputStream = Files.newBufferedWriter(Paths.get(filename), StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            logger.error("FileSinkTask: Unable to open file '{}'. Error {}", filename, e.getMessage());
            throw new ConnectException("FileSinkTask: Unable to open file '" + filename + "'. Error " + e.getMessage());
        }
    }

    @Override
    public void put(Collection<SinkRecord> collection) {
        for (SinkRecord record : collection) {
            try {
                logger.trace("Writing line to {}: {}", filename, record.value());
                outputStream.append(record.value().toString());
                outputStream.newLine();
            } catch (IOException e) {
                logger.error("FileSinkTask: Unable to write file '{}'. Error {}", filename, e.getMessage());
                throw new ConnectException(e);
            }
        }
    }

    @Override
    public void stop() {
        logger.debug("Stopping task...");
        if (outputStream == null) {
            return;
        }
        try {
            outputStream.flush();
            outputStream.close();
        } catch (IOException e) {
            logger.error("FileSinkTask: Unable to close/flush file '{}'", filename);
            throw new ConnectException("FileSinkTask: Unable to close file '" + filename + "'");
        }
    }

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }
}
