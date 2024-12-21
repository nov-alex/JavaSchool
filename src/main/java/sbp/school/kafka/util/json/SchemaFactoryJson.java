package sbp.school.kafka.util.json;

import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;

/**
 * Фабрика валидации json по схеме
 */
public final class SchemaFactoryJson {

    private static JsonSchemaFactory factory;

    private SchemaFactoryJson() {
        // No_OP
    }

    public static JsonSchemaFactory getJsonSchemaFactory() {
        if (factory == null) {
            factory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V4);
        }
        return factory;
    }
}
