package sbp.school.kafka.source;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sbp.school.kafka.entity.TransactionData;
import sbp.school.kafka.entity.TransactionType;
import sbp.school.kafka.util.json.ObjectMapperJson;

import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static sbp.school.kafka.source.DbSourceConnector.*;

public class DbSourceTask extends SourceTask {

    private static final Logger logger = LoggerFactory.getLogger(DbSourceTask.class);

    public static final String DB_TABLE_FIELD = "dbTable";
    public static final String POSITION_FIELD = "position";
    private static final Schema VALUE_SCHEMA = Schema.STRING_SCHEMA;
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");


    private long dbOffset = 0L;
    private String url;
    private String user;
    private String password;
    private String db_table;
    private String topic;
    private Connection connection;

    public DbSourceTask() {
        //NO_OP
    }

    public DbSourceTask(Connection connection) {
        this.connection = connection;
    }

    @Override
    public void start(Map<String, String> props) {
        logger.info("Starting JDBC source task");
        AbstractConfig config = new AbstractConfig(DbSourceConnector.CONFIG_DEF, props);
        String driver = config.getString(DB_DRIVER);
        url = config.getString(DB_URL);
        user = config.getString(DB_USER);
        password = config.getString(DB_PASSWORD);
        db_table = config.getString(DB_TABLE);
        topic = config.getString(TOPIC);

        try {
            DriverManager.registerDriver((Driver) Class.forName(driver)
                    .getDeclaredConstructor()
                    .newInstance());
            connection = DriverManager.getConnection(url, user, password);
        } catch (SQLException | IllegalAccessException | ClassNotFoundException | InstantiationException |
                 InvocationTargetException | NoSuchMethodException e) {
            logger.error(e.getMessage());
            throw new ConnectException(e.getMessage());
        }
    }

    @Override
    public List<SourceRecord> poll() {
        Map<String, Object> offset = context.offsetStorageReader().offset(Collections.singletonMap(DB_TABLE_FIELD, db_table));
        if (offset != null) {
            Object lastRecordedOffset = offset.get(POSITION_FIELD);
            if (lastRecordedOffset != null && !(lastRecordedOffset instanceof Long))
                throw new ConnectException("Offset position is the incorrect type");
            dbOffset = (lastRecordedOffset != null) ? (Long) lastRecordedOffset : 0L;
        } else {
            dbOffset = 0L;
        }
        logger.debug("Fetch from DB from offset {}", dbOffset);

        List<TransactionData> transactions = getTransactions(dbOffset);
        return transactions
                .stream()
                .map(this::createSourceRecord)
                .filter(Objects::nonNull)
                .toList();
    }

    @Override
    public void stop() {
        logger.info("Closing db connection...");
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                logger.info("Error closing db connection: {}", e.getMessage());
                throw new ConnectException(e);
            }
        }
    }

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    private Map<String, String> offsetKey(String filename) {
        return Collections.singletonMap(DB_TABLE_FIELD, filename);
    }

    private Map<String, Long> offsetValue(Long pos) {
        return Collections.singletonMap(POSITION_FIELD, pos);
    }

    private SourceRecord createSourceRecord(TransactionData transactionData) {
        try {
            String value = ObjectMapperJson.getObjectMapper().writeValueAsString(transactionData);
            return new SourceRecord(
                    offsetKey(this.db_table),
                    offsetValue(this.dbOffset),
                    this.topic,
                    null,
                    null,
                    null,
                    VALUE_SCHEMA,
                    value,
                    System.currentTimeMillis()
            );
        } catch (JsonProcessingException e) {
            logger.info("Unable create json: {}", e.getMessage());
            return null;
        }
    }

    private List<TransactionData> getTransactions(Long dbOffset) {
        List<TransactionData> result = new ArrayList<>();
        try (PreparedStatement statement = connection.prepareStatement("select * from " + db_table + " offset ?")) {
            statement.setLong(1, dbOffset);
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                UUID id = UUID.fromString(resultSet.getString("id"));
                TransactionType transactionType = TransactionType.valueOf(resultSet.getString("transactionType"));
                BigDecimal sum = new BigDecimal(resultSet.getString("amount"));
                LocalDateTime date = LocalDateTime.parse(resultSet.getString("date").replaceAll("\\.\\d+$", ""), formatter);
                String accountNumber = resultSet.getString("accountNumber");
                result.add(new TransactionData(id, transactionType, sum, date, accountNumber));
            }
        } catch (Exception e) {
            logger.error("BD fetch error: {}", e.getMessage());
            throw new ConnectException(e.getMessage());
        }
        return result;
    }
}
