package sbp.school.kafka.entity;

/**
 * Тип транзакции
 */
public enum TransactionType {
    TRANSACTION_DEBIT,      // Операция "Списание"
    TRANSACTION_CREDIT,     // Операция "Зачисление"
    TRANSACTION_CHARGEBACK  // Операция "Возврат"
}
