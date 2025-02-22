package sbp.school.kafka.storage.dto;


import sbp.school.kafka.entity.TransactionData;
import sbp.school.kafka.storage.enums.AckStatus;

public class AckStorageData {
    private AckStatus ackStatus;
    private TransactionData data;

    public AckStatus getAckStatus() {
        return ackStatus;
    }

    public AckStorageData setAckStatus(AckStatus ackStatus) {
        this.ackStatus = ackStatus;
        return this;
    }

    public TransactionData getData() {
        return data;
    }

    public AckStorageData setData(TransactionData data) {
        this.data = data;
        return this;
    }

    @Override
    public String toString() {
        return "AckStorageData{" +
                "ackStatus=" + ackStatus +
                ", data=" + data +
                '}';
    }
}
