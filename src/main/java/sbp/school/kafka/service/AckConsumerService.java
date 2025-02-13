package sbp.school.kafka.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sbp.school.kafka.entity.AckData;
import sbp.school.kafka.entity.TransactionData;
import sbp.school.kafka.storage.AckStorage;
import sbp.school.kafka.storage.dto.AckStorageData;
import sbp.school.kafka.storage.enums.AckStatus;
import sbp.school.kafka.util.ack.HashUtils;

import java.util.Arrays;
import java.util.List;

/**
 * Сервис-потребитель для обработки подтверждений отправки
 */
public class AckConsumerService {

    private static final Logger logger = LoggerFactory.getLogger(AckConsumerService.class);

    private final AckStorage ackStorage;
    private final ProducerService<TransactionData> producer;

    public AckConsumerService(ProducerService<TransactionData> producer, AckStorage ackStorage) {
        this.producer = producer;
        this.ackStorage = ackStorage;
    }

    public void ackProcessor(AckData ackData) {
        logger.trace("Сравнение hash: start {}, end {}, hash '{}' ", ackData.start(), ackData.end(), Arrays.toString(ackData.hash()));

        List<AckStorageData> getStorageDataByTimeSlice = ackStorage.getStorageDataByTimeSlice(ackData.start(), ackData.end());
        byte[] expectedHash = HashUtils.calculateHash(getStorageDataByTimeSlice);

        if (Arrays.equals(expectedHash, ackData.hash())) {
            logger.debug("Hash совпали: start {}, end {}, hash '{}' ", ackData.start(), ackData.end(), Arrays.toString(ackData.hash()));
            ackStorage.confirmAckByTimeSlice(ackData.start(), ackData.end());
        } else {
            logger.error("Hash отличаются: start {}, end {}, hash '{}', hash_expected '{}'  ", ackData.start(),
                    ackData.end(),
                    Arrays.toString(ackData.hash()),
                    Arrays.toString(expectedHash));

            ackStorage.updateAckByTimeSlice(ackData.start(), ackData.end(), AckStatus.ACK_INVALID);

            resend();
        }
    }

    public void resend() {
        List<TransactionData> corruptedData = ackStorage.poll();
        corruptedData.forEach(producer::resend);
    }
}
