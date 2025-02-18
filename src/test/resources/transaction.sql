DROP TABLE IF EXISTS `transactions`;
CREATE TABLE IF NOT EXISTS `transactions` (
    id UUID PRIMARY KEY,
    transactionType VARCHAR(255) NOT NULL,
    amount NUMERIC(20, 2),
    date TIMESTAMP,
    accountNumber VARCHAR(255) NOT NULL
);

INSERT INTO `transactions` (id, transactionType, amount, date, accountNumber) VALUES ('2D1EBC5B7D2741979CF0E84451C5BBB1', 'TRANSACTION_DEBIT', 5.0, {ts '2012-09-17 18:47:52'}, '12345678915874222550');