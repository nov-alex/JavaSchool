package sbp.school.kafka.storage;

import sbp.school.kafka.entity.TransactionData;
import sbp.school.kafka.storage.dto.AckStorageData;
import sbp.school.kafka.storage.enums.AckStatus;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

/**
 * Хранилище для переданных данных производителем
 * для сервиса подтверждения
 */
public class AckStorage {

    private final List<AckStorageData> storage;

    public AckStorage() {
        this.storage = new ArrayList<>();
    }

    public void put(TransactionData transactionData) {
        storage.add(new AckStorageData()
                .setData(transactionData)
                .setAckStatus(AckStatus.ACK_PENDING));
    }

    public List<TransactionData> poll() {
        return storage
                .stream()
                .filter(data -> data.getAckStatus() == AckStatus.ACK_INVALID)
                .map(data -> data.setAckStatus(AckStatus.ACK_PENDING))
                .map(AckStorageData::getData)
                .toList();
    }

    public List<AckStorageData> getStorageDataByTimeSlice(LocalDateTime begin, LocalDateTime end) {
        return storage
                .stream()
                .filter(data -> data.getData().date().isBefore(begin) && data.getData().date().isAfter(end))
                .toList();
    }

    public void confirmAckByTimeSlice(LocalDateTime begin, LocalDateTime end) {
        updateAckByTimeSlice(begin, end, AckStatus.ACK_CONFIRMED);
    }

    public void updateAckByTimeSlice(LocalDateTime begin, LocalDateTime end, AckStatus ackStatus) {
        storage.stream().filter(data ->
                        data.getData().date().isBefore(begin) && data.getData().date().isAfter(end))
                .forEach(data -> data.setAckStatus(ackStatus));
    }

}
