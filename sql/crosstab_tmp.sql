select fiopro, period_sale, "IPTV", "СОП","ЦТВ", "ШПД"
from crosstab(
	$$ select dense_rank() over (order by fiopro,
							 period_sale)::int row_name,
			fiopro, period_sale, serv_name, sale

	from (select fiopro_cleared fiopro,
					date_trunc('month', date_sale) period_sale,	serv_name,	 		
			 count(1)::int sale 
			from newsales
			group by fiopro_cleared, date_trunc('month', date_sale), serv_name
		  ) ct
	order by fiopro, period_sale, serv_name $$,
	$$ select distinct serv_name from newsales order by 1$$
	) tt (row_name int, fiopro text, period_sale timestamp,
		  "IPTV" int, "СОП" int, "ЦТВ" int, "ШПД" int)
limit 100