with fio_id as (
select distinct fiopro, "key", channel_detail, period_date_sale,sale,
	row_number() over (partition by fiopro, period_date_sale  order by sale desc) as rk
from (
		select fiopro, "key", channel_detail, period_date_sale, sum(newsale) as sale
			
	from scource	
	group by fiopro, "key", channel_detail, period_date_sale

) as foo
	)
 
select *
from fio_id as f
where f.rk = 1 and fiopro ='РОМАНОВ ДМИТРИЙ ИГОРЕВИЧ'
order by period_date_sale 