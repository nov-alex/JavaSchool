package sbp.school.kafka.util.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.ValidationMessage;
import sbp.school.kafka.exception.ApplicationValueDeserializerException;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Валидатор схемы json
 */
public class TransactionDataJsonValidator {

    private static final String TRANSACTION_DATA_SCHEMA = "transactiondata-schema.json";
    private static final JsonSchema jsonSchema = readJsonSchema();

    public static void validate(String jsonTransactionData) throws ApplicationValueDeserializerException {
        JsonNode jsonNode;
        ObjectMapper mapper = ObjectMapperJson.getObjectMapper();
        try {
            jsonNode = mapper.readTree(jsonTransactionData);
        } catch (JsonProcessingException e) {
            throw new ApplicationValueDeserializerException(e);
        }
        Set<ValidationMessage> errors = jsonSchema.validate(jsonNode);
        if (!errors.isEmpty()) {
            throw new ApplicationValueDeserializerException("Валидация json: " + errors
                    .stream()
                    .map(ValidationMessage::toString)
                    .collect(Collectors.joining(";")));
        }
    }

    private static InputStream getInputStream() {
        return TransactionDataJsonValidator.class.getClassLoader().getResourceAsStream(TRANSACTION_DATA_SCHEMA);
    }

    private static JsonSchema readJsonSchema() {
        try (InputStream inputStream = getInputStream()) {
            if (inputStream == null) {
                throw new ApplicationValueDeserializerException("Не удается открыть файл " + TRANSACTION_DATA_SCHEMA);
            }
            BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);
            return SchemaFactoryJson.getJsonSchemaFactory().getSchema(bufferedInputStream);
        } catch (IOException e) {
            throw new ApplicationValueDeserializerException(e);
        }
    }
}
