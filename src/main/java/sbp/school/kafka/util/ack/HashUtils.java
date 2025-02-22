package sbp.school.kafka.util.ack;

import sbp.school.kafka.exception.ApplicationHashException;
import sbp.school.kafka.storage.dto.AckStorageData;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.stream.Collectors;

public class HashUtils {

    public static byte[] calculateHash(List<AckStorageData> data) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            String concatData = data.stream()
                    .map(ackData -> ackData.getData().toAckHashString())
                    .collect(Collectors.joining(""));
            return digest.digest(concatData.getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException e) {
            throw new ApplicationHashException(e.getMessage());
        }
    }
}
