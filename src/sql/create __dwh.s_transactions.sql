drop table if exists STV2024111152__DWH.s_transactions
	
create table IF NOT EXISTS STV2024111152__DWH.s_transactions (
	hk_operation_id int not null CONSTRAINT fk_s_transactions_h_transactions REFERENCES STV2024111152__DWH.h_transactions (hk_operation_id),
	account_number_from int NULL,
	account_number_to int NULL,
	currency_code int NULL,
	country varchar(30) NULL,
	status varchar(30) NULL,
	transaction_type varchar(30) NULL,
	amount int NULL,
	transaction_dt timestamp NULL,
	load_dt datetime not null,
    load_src varchar(60))