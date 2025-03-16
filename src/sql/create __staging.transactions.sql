drop table if exists STV2024111152__STAGING.transactions
delete from STV2024111152__STAGING.transactions

create table IF NOT EXISTS STV2024111152__STAGING.transactions (
	operation_id varchar(60) NULL,
	account_number_from int NULL,
	account_number_to int NULL,
	currency_code int NULL,
	country varchar(30) NULL,
	status varchar(30) NULL,
	transaction_type varchar(30) NULL,
	amount int NULL,
	transaction_dt timestamp NULL,
	CONSTRAINT C_PRIMARY PRIMARY KEY (operation_id) DISABLED)
	PARTITION BY transaction_dt::date
	GROUP BY transaction_dt


create PROJECTION STV2024111152__STAGING.transactions_proj (
 operation_id,
 account_number_from,
 account_number_to,
 currency_code,
 country,
 status,
 transaction_type,
 amount,
 transaction_dt)
as select 
		transactions.operation_id,
        transactions.account_number_from,
        transactions.account_number_to,
        transactions.currency_code,
        transactions.country,
        transactions.status,
        transactions.transaction_type,
        transactions.amount,
        transactions.transaction_dt
 from STV2024111152__STAGING.transactions
 order by transactions.transaction_dt,
          transactions.operation_id
 segmented by hash(transactions.transaction_dt, transactions.operation_id) all nodes KSAFE 1