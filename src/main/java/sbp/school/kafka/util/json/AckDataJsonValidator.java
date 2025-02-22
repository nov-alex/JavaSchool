package sbp.school.kafka.util.json;

public class AckDataJsonValidator {

    public static final JsonValidatorImpl VALIDATOR =
            new JsonValidatorImpl(JsonValidatorImpl.readJsonSchema("ackdata-schema.json"));

    public static void validate(String data) {
        VALIDATOR.validate(data);
    }

}
