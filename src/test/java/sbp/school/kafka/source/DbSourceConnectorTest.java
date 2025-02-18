package sbp.school.kafka.source;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DbSourceConnectorTest {

    @Test
    public void testTaskClass() {
        DbSourceConnector connector = new DbSourceConnector();
        assertEquals(DbSourceTask.class, connector.taskClass());
    }

}