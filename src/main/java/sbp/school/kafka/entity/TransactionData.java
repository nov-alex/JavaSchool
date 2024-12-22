package sbp.school.kafka.entity;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.UUID;

/**
 * @param id              Идентификатор сообщения
 * @param transactionType Тип транзакции
 * @param sum             Сумма транзакции
 * @param date            Дата транзакции
 * @param accountNumber   Номер счета
 */
public record TransactionData(UUID id,
                              TransactionType transactionType,
                              BigDecimal sum,
                              LocalDate date,
                              String accountNumber) {
}
