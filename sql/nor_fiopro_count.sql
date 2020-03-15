with temp as (select distinct UPPER(fiopro_old) as fiopro, 
			  UPPER(fiopro) as fiopro_cleared,
			  min(ddate_date_sale) as min_date_sale
from scource
where levenshtein(UPPER(fiopro_old),UPPER(fiopro))>2
group by UPPER(fiopro_old), UPPER(fiopro)
order by UPPER(fiopro_old)
			  
)

select fiopro_cleared, count(fiopro), min(min_date_sale)
from temp
group by fiopro_cleared
order by count(fiopro)