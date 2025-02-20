package sbp.school.kafka.entity;

import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.time.LocalDateTime;
import java.util.UUID;

/**
 * @param id              Идентификатор сообщения
 * @param transactionType Тип транзакции
 * @param sum             Сумма транзакции
 * @param date            Дата и время транзакции
 * @param accountNumber   Номер счета
 */
public record TransactionData(UUID id,
                              TransactionType transactionType,
                              BigDecimal sum,
                              LocalDateTime date,
                              String accountNumber) {

    @Override
    public String toString() {
        return "TransactionData{" +
                "id=" + id +
                ", transactionType=" + transactionType +
                ", sum=" + sum +
                ", date=" + date +
                ", accountNumber='" + accountNumber + '\'' +
                '}';
    }

    public String toAckHashString() {
        return id.toString() +
                transactionType.toString() +
                formatBd(sum) +
                date.toString() +
                accountNumber;
    }

    private String formatBd(BigDecimal bd) {
        DecimalFormat df = new DecimalFormat();
        df.setMaximumFractionDigits(4);
        df.setMinimumFractionDigits(4);
        df.setGroupingUsed(false);

        return df.format(bd);
    }
}
