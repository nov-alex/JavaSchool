package sbp.school.kafka.util.json;

public class TransactionDataJsonValidator {

    public static final JsonValidatorImpl VALIDATOR =
            new JsonValidatorImpl(JsonValidatorImpl.readJsonSchema("transactiondata-schema.json"));

    public static void validate(String data){
        VALIDATOR.validate(data);
    }

}
