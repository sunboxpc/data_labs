with grid as (
-- создаем для каждого агента календарь с момент первой продажи и запоминаем этот период по период последней продажи
	select fiopro_cleared, period_sale,  min_date_sale
	from (	
		select distinct upper(fiopro_cleared) fiopro_cleared, 
						min(date_trunc('month', date_sale)) min_date_sale,
						max(date_trunc('month', date_sale)) max_date_sale
		from newsales
		group by upper(fiopro_cleared)) s
	inner join (
		select cast('2018-01-01' as date) + (n || 'month'):: interval period_sale
	   	from generate_series(0,24) n) d
		on 1=1 
	where period_sale >= min_date_sale
		and period_sale <= max_date_sale
		and length(fiopro_cleared) > 1		
	order by fiopro_cleared, period_sale
),
sale_tbl as (
-- собираем факт продаж по периодам и агентам
	select  upper(fiopro_cleared) fiopro_cleared,
			date_trunc('month', date_sale) period_sale,
			count(1) sale
	from newsales
	group by upper(fiopro_cleared),date_trunc('month', date_sale)
)

--CREATE table agent_sales as
select id, fiopro_cleared, period_sale, min_date_sale, cur_period_sale, status
--into agent_sales
from (
	select row_number() over () as id, 
			fiopro_cleared, period_sale, 
			min_date_sale, sale cur_period_sale,
			(case when sale is null then false
			else true end) status
from grid l
full join sale_tbl r using(fiopro_cleared, period_sale)
--where fiopro_cleared like 'КАТАЕВ ИГОРЬ%'
) t
--offset 9
--limit 1000