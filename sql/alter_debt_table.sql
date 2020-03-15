--update debt set sale_date=to_date(to_char(sales_date, 'YYYYDDMM'), 'YYYYMMDD')
--alter table debt add column sale_period date
--update debt set sale_period = date_trunc('month',sale_date)
--alter table drop column sales_date

select distinct sales_date, sale_date, sale_period from debt