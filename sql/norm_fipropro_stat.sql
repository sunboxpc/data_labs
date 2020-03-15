with temp as (select distinct UPPER(fiopro) as fiopro, 
			  UPPER(fiopro_cleared) as fiopro_cleared,
			  min(date_sale) as min_date_sale,
			  date_trunc('month',date_sale) as period_sale,
			  channel,
			  count(subs_id) as sales
from newsales
--where levenshtein(UPPER(fiopro),UPPER(fiopro_cleared))>2
group by UPPER(fiopro), UPPER(fiopro_cleared), date_trunc('month',date_sale),channel
order by UPPER(fiopro_cleared)
			  
)

select fiopro_cleared, fiopro, min_date_sale, period_sale,sales, channel
from temp
--group by fiopro_cleared
--order by count(fiopro)