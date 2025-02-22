package sbp.school.kafka.sink;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import sbp.school.kafka.config.Configuration;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.h2.store.fs.FileUtils.delete;
import static org.junit.jupiter.api.Assertions.assertEquals;

class FileSinkTaskTest {

    private FileSinkTask task;
    private Map<String, String> config;
    private final String outputFile = Configuration.loadProperty("connector-sink.properties").getProperty("filename");

    @BeforeEach
    public void setup() {
        config = new HashMap<>();
        config.put(FileSinkConnector.FILENAME, outputFile);
        task = new FileSinkTask();
    }

    @Test
    public void testStart() throws IOException {
        task = new FileSinkTask();

        task.start(config);

        task.put(List.of(
                new SinkRecord("topic1", 0, null, null, Schema.STRING_SCHEMA, "line0", 2),
                new SinkRecord("topic1", 0, null, null, Schema.STRING_SCHEMA, "line1", 2)
        ));

        int numLines = 2;
        String[] lines = new String[numLines];
        int i = 0;
        task.stop();
        try (BufferedReader reader = Files.newBufferedReader(Paths.get(outputFile), StandardCharsets.UTF_8)) {
            lines[i++] = reader.readLine();
            lines[i++] = reader.readLine();
        } finally {
            delete(outputFile);
        }

        while (--i >= 0) {
            assertEquals("line" + i, lines[i]);
        }
    }

}