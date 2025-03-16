drop table if exists STV2024111152__DWH.h_transactions
	
create table IF NOT EXISTS STV2024111152__DWH.h_transactions (
	hk_operation_id int not null,
	operation_id varchar(60) NULL,
	load_dt datetime not null,
    load_src varchar(60),
    constraint c_h_сurrencies_pk primary key (hk_operation_id) ENABLED)



	