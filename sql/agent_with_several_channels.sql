with foo as (
	select fiopro_cleared, fiopro, filial, 
	  string_agg(distinct channel, chr(10) order by channel) as channel 
	  from newsales
	group by fiopro_cleared,fiopro, filial
)

select *
from foo
where fiopro_cleared in (
			select fiopro_cleared 
			from foo
			group by fiopro_cleared
			having count(1) >10)
		and channel like '%Активный%'


