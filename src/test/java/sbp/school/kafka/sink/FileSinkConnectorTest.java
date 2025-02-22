package sbp.school.kafka.sink;

import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.sink.SinkConnector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import sbp.school.kafka.config.Configuration;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

class FileSinkConnectorTest {

    private static final String TOPIC = "test";
    private static final String FILENAME = Configuration.loadProperty("connector-sink.properties").getProperty("filename");

    private FileSinkConnector connector;
    private Map<String, String> sinkProperties;

    @BeforeEach
    public void setup() {
        connector = new FileSinkConnector();
        ConnectorContext ctx = mock(ConnectorContext.class);
        connector.initialize(ctx);

        sinkProperties = new HashMap<>();
        sinkProperties.put(SinkConnector.TOPICS_CONFIG, TOPIC);
        sinkProperties.put(FileSinkConnector.FILENAME, FILENAME);
    }

    @Test
    public void testTaskClass() {
        connector.start(sinkProperties);
        assertEquals(FileSinkTask.class, connector.taskClass());
    }

    @Test
    public void testSinkTasks() {
        connector.start(sinkProperties);
        List<Map<String, String>> taskConfigs = connector.taskConfigs(1);
        assertEquals(1, taskConfigs.size());
        assertEquals(FILENAME, taskConfigs.get(0).get(FileSinkConnector.FILENAME));

        taskConfigs = connector.taskConfigs(2);
        assertEquals(2, taskConfigs.size());
        for (int i = 0; i < 2; i++) {
            assertEquals(FILENAME, taskConfigs.get(0).get(FileSinkConnector.FILENAME));
        }
    }

}