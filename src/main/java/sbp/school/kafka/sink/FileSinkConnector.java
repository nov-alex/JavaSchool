package sbp.school.kafka.sink;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sbp.school.kafka.config.Configuration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FileSinkConnector extends SinkConnector {

    private static final Logger logger = LoggerFactory.getLogger(FileSinkConnector.class);

    public static final String FILENAME = "filename";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(FILENAME, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Filename for sink");

    public static final Map<String, String> props = new HashMap<>();

    static {
        props.put(FILENAME, Configuration.loadProperty("connector-sink.properties").getProperty("filename"));
    }

    public static final Map<String, Object> CONFIGS = CONFIG_DEF.parse(props);

    @Override
    public void start(Map<String, String> map) {
        AbstractConfig config = new AbstractConfig(CONFIG_DEF, props);
        String filename = config.getString(FILENAME);
        logger.info("Starting file sink connector writing to {}", filename);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return FileSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> taskConfigs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            taskConfigs.add(props);
        }
        return taskConfigs;
    }

    @Override
    public void stop() {
        //NO_OP
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }
}
