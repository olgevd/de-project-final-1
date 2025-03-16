create table IF NOT EXISTS STV2024111152__STAGING.сurrencies (
	date_update timestamp NULL,
	currency_code int NULL,
	currency_code_with int NULL,
	currency_with_div numeric(5, 3) NULL)
	PARTITION BY date_update::date

	
create PROJECTION STV2024111152__STAGING.сurrencies_proj (
	date_update,
	currency_code,
	currency_code_with,
	currency_with_div)
as select date_update,
	currency_code,
	currency_code_with,
	currency_with_div
 from STV2024111152__STAGING.сurrencies
 order by currencies.date_update, currency_code, currency_code_with
 segmented by hash(currencies.date_update, currencies.currency_code) all nodes KSAFE 1;