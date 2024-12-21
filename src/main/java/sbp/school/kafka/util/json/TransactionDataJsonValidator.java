package sbp.school.kafka.util.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.ValidationMessage;
import sbp.school.kafka.exception.ApplicationSerializationException;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Set;
import java.util.stream.Collectors;

public class TransactionDataJsonValidator {

    private static final String TRANSACTION_DATA_SCHEMA = "transactiondata-schema.json";
    private final JsonSchema jsonSchema;

    public TransactionDataJsonValidator(JsonSchema jsonSchema) {
        this.jsonSchema = jsonSchema;
    }

    public void validate(String jsonTransactionData) throws ApplicationSerializationException {
        JsonNode jsonNode;
        ObjectMapper mapper = ObjectMapperJson.getObjectMapper();
        try {
            jsonNode = mapper.readTree(jsonTransactionData);
        } catch (JsonProcessingException e) {
            throw new ApplicationSerializationException(e);
        }
        Set<ValidationMessage> errors = jsonSchema.validate(jsonNode);
        if (!errors.isEmpty()) {
            throw new ApplicationSerializationException("Валидация json: " + errors
                    .stream()
                    .map(ValidationMessage::toString)
                    .collect(Collectors.joining(";")));
        }
    }

    private static InputStream getInputStream() {
        return TransactionDataJsonValidator.class.getClassLoader().getResourceAsStream(TRANSACTION_DATA_SCHEMA);
    }

    public static JsonSchema readJsonSchema() {
        try (InputStream inputStream = getInputStream()) {
            if (inputStream == null) {
                throw new ApplicationSerializationException("Не удается открыть файл " + TRANSACTION_DATA_SCHEMA);
            }
            BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);
            return SchemaFactoryJson.getJsonSchemaFactory().getSchema(bufferedInputStream);
        } catch (IOException e) {
            throw new ApplicationSerializationException(e);
        }
    }
}
