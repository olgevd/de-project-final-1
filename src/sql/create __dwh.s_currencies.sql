drop table if exists STV2024111152__DWH.s_currencies
	
create table IF NOT EXISTS STV2024111152__DWH.s_currencies (
	hk_currency_id int not null CONSTRAINT fk_s_currencies_h_currencies REFERENCES STV2024111152__DWH.h_currencies (hk_currency_id),
	date_update timestamp NULL,
	currency_code_with int NULL,
	currency_with_div numeric(5, 3) NULL,
	load_dt datetime not null,
    load_src varchar(60))
    


	