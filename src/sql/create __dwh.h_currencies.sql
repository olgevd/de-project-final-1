drop table if exists STV2024111152__DWH.h_currencies
	
create table IF NOT EXISTS STV2024111152__DWH.h_currencies (
	hk_currency_id int not null,
	currency_code int null,
	load_dt datetime not null,
    load_src varchar(60),
    constraint c_h_сurrencies_pk primary key (hk_currency_id) ENABLED)
    