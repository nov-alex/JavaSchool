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
public class JsonValidatorImpl implements JsonValidator {

    private final JsonSchema jsonSchema;

    public JsonValidatorImpl(JsonSchema jsonSchema) {
        this.jsonSchema = jsonSchema;
    }

    @Override
    public void validate(String jsonData) throws ApplicationValueDeserializerException {
        JsonNode jsonNode;
        ObjectMapper mapper = ObjectMapperJson.getObjectMapper();
        try {
            jsonNode = mapper.readTree(jsonData);
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

    private static InputStream getInputStream(String resourceFileName) {
        return JsonValidatorImpl.class.getClassLoader().getResourceAsStream(resourceFileName);
    }

    public static JsonSchema readJsonSchema(String resourceFileName) {
        try (InputStream inputStream = getInputStream(resourceFileName)) {
            if (inputStream == null) {
                throw new ApplicationValueDeserializerException("Не удается открыть файл " + resourceFileName);
            }
            BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);
            return SchemaFactoryJson.getJsonSchemaFactory().getSchema(bufferedInputStream);
        } catch (IOException e) {
            throw new ApplicationValueDeserializerException(e);
        }
    }
}
