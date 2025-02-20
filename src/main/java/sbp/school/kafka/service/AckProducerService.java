package sbp.school.kafka.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sbp.school.kafka.entity.AckData;
import sbp.school.kafka.storage.AckStorage;
import sbp.school.kafka.storage.dto.AckConfigDto;
import sbp.school.kafka.storage.dto.AckStorageData;
import sbp.school.kafka.util.ack.HashUtils;

import java.time.LocalDateTime;
import java.util.List;

public class AckProducerService implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(AckProducerService.class);

    private final AckStorage ackStorage;
    private final ProducerService<AckData> producer;
    private final AckConfigDto config;
    private LocalDateTime startTimeSlice = LocalDateTime.now();

    public AckProducerService(ProducerService<AckData> producer, AckStorage ackStorage, AckConfigDto config) {
        this.producer = producer;
        this.ackStorage = ackStorage;
        this.config = config;
    }


    public void service() {

        LocalDateTime end = LocalDateTime.now().minus(config.getTimeDelaySec());
        if (end.isAfter(startTimeSlice)) {
            logger.debug("Наложение интервала, отмена отправки ack: start {}, end {}", startTimeSlice, end );
            return;
        }

        List<AckStorageData> dataList = ackStorage.getStorageDataByTimeSlice(startTimeSlice, end);
        logger.trace("Подготовка к отправке ack: start {}, end {}, транзакции {}", startTimeSlice, end, dataList.size());

        AckData ackData = new AckData(startTimeSlice, end, HashUtils.calculateHash(dataList));
        startTimeSlice = end;
        if (dataList.isEmpty()) {
            return;
        }

        logger.debug("Отправка ack: start {}, end {}", ackData.start(), ackData.end());

        producer.resend(ackData);
    }

    @Override
    public void run() {
        try {
            Thread.sleep(60000L);
            service();
            if (!Thread.currentThread().isInterrupted()) {
                throw new InterruptedException();
            }
        } catch (InterruptedException e) {
            System.out.println("Ack producer service shutdown...");
        }
    }

    public void stop() {
        Thread.currentThread().interrupt();
    }
}
