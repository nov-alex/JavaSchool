package sbp.school.kafka.source;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.h2.tools.RunScript;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DbSourceTaskTest {

    private final String DRIVER = "org.h2.Driver";
    private final String URL = "jdbc:h2:mem:testdb";
    private final String USER = "admin";
    private final String PASSWORD = "";
    private DbSourceTask task;
    private SourceTaskContext context;
    private OffsetStorageReader offsetStorageReader;
    Map<String, String> config;

    @BeforeEach
    public void setup() throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException, SQLException {
        DriverManager.registerDriver((Driver) Class.forName(DRIVER)
                .getDeclaredConstructor()
                .newInstance());

        config = new HashMap<>();
        config.put(DbSourceConnector.DB_DRIVER, DRIVER);
        config.put(DbSourceConnector.DB_URL, URL);
        config.put(DbSourceConnector.DB_USER, USER);
        config.put(DbSourceConnector.DB_PASSWORD, PASSWORD);
        config.put(DbSourceConnector.DB_TABLE, "transactions");
        config.put(DbSourceConnector.TOPIC, "quickstart-sber-demo");


        context = mock(SourceTaskContext.class);
        offsetStorageReader = mock(OffsetStorageReader.class);
    }

    @Test
    public void testStart() {

        task = new DbSourceTask();

        Assertions.assertDoesNotThrow(() -> task.start(config));

    }

    @Test
    public void testPoll() throws SQLException, FileNotFoundException {
        try (Connection connection = DriverManager.getConnection(URL, USER, PASSWORD)) {

            RunScript.execute(connection, new FileReader("src/test/resources/transaction.sql"));

            expectOffsetLookupReturnNone();

            task = new DbSourceTask(connection);
            task.initialize(context);
            task.start(config);

            List<SourceRecord> actual = task.poll();
            assertEquals(1, actual.size());
        }
    }

    private void expectOffsetLookupReturnNone() {
        when(context.offsetStorageReader()).thenReturn(offsetStorageReader);
        when(offsetStorageReader.offset(anyMap())).thenReturn(null);
    }
}
