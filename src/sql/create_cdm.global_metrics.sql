drop table if exists STV2024111152__DWH.global_metrics
	
create table IF NOT EXISTS STV2024111152__DWH.global_metrics (
	date_update timestamp not NULL,
	currency_from varchar not NULL,
	amount_total numeric(14,2) NOT NULL,
	cnt_transactions int NULL,
	avg_transactions_per_account numeric(15,2) NOT NULL,
	cnt_accounts_make_transactions int NOT NULL)