drop table if exists STV2024111152__DWH.l_transactions_currencies
	
create table IF NOT EXISTS STV2024111152__DWH.l_transactions_currencies (
	hk_transactions_currencies bigint primary key,
	hk_operation_id int not null CONSTRAINT fk_l_transactions_currencies_h_transactions REFERENCES STV2024111152__DWH.h_transactions (hk_operation_id),
	hk_currency_id int not null CONSTRAINT fk_l_transactions_currencies_h_currencies REFERENCES STV2024111152__DWH.h_currencies (hk_currency_id),
	load_dt datetime not null,
    load_src varchar(60))