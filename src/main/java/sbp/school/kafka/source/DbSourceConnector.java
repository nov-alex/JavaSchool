package sbp.school.kafka.source;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sbp.school.kafka.config.Configuration;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DbSourceConnector extends SourceConnector {

    private static final Logger logger = LoggerFactory.getLogger(DbSourceConnector.class);

    public static final String TOPIC = Configuration.loadProperty("producer-topic.properties").getProperty("topic");
    public static final String DB_DRIVER = Configuration.loadProperty("database.properties").getProperty("db.driver");
    public static final String DB_URL = Configuration.loadProperty("database.properties").getProperty("db.url");
    public static final String DB_USER = Configuration.loadProperty("database.properties").getProperty("db.username");
    public static final String DB_PASSWORD = Configuration.loadProperty("database.properties").getProperty("db.password");
    public static final String DB_TABLE = Configuration.loadProperty("database.properties").getProperty("db.table.name");

    static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(TOPIC, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, new ConfigDef.NonEmptyString(), ConfigDef.Importance.HIGH, "The topic to publish data to")
            .define(DB_DRIVER, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, new ConfigDef.NonEmptyString(), ConfigDef.Importance.HIGH, "Name of JDBC Driver")
            .define(DB_URL, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, new ConfigDef.NonEmptyString(), ConfigDef.Importance.HIGH, "JDBC Driver connection url")
            .define(DB_USER, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, new ConfigDef.NonEmptyString(), ConfigDef.Importance.HIGH, "DB username")
            .define(DB_PASSWORD, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH, "DB password")
            .define(DB_TABLE, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, new ConfigDef.NonEmptyString(), ConfigDef.Importance.HIGH, "DB table");

    private Map<String, String> props;

    @Override
    public void start(Map<String, String> props) {
        this.props = props;
        AbstractConfig config = new AbstractConfig(CONFIG_DEF, props);
        logger.info("Starting JDBC Source Connector");
    }

    @Override
    public Class<? extends Task> taskClass() {
        return DbSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTask) {
        ArrayList<Map<String, String>> configs = new ArrayList<>();
        // Only one input stream makes sense.
        configs.add(props);
        return configs;
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
