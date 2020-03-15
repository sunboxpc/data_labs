with grid_tbl as (
-- создаем для каждого агента последовательность месяцев
-- с первой по крайнюю продажу и запоминаем минимальный и максимальный период продажи
	select s.fiopro_cleared, s.filial, s.min_date_sale, s.max_date_sale,d.period_sale, s.channel
	from (	
		select distinct upper(fiopro_cleared) fiopro_cleared, filial,
						string_agg(distinct channel, chr(10) order by channel) as channel,
						min(date_trunc('month', date_sale)) min_date_sale,
						max(date_trunc('month', date_sale)) max_date_sale
		from newsales
		group by upper(fiopro_cleared), filial
		) s
	inner join (
		select cast('2018-01-01' as date) + (n || 'month'):: interval period_sale
	   	from generate_series(0,24) n
		) d
		on 1=1 
	where period_sale >= min_date_sale
		and period_sale <= max_date_sale
		and length(fiopro_cleared) > 1		
	order by fiopro_cleared, period_sale
)
/*sale_tbl as (
-- собираем факт продаж по периодам и агентам
	select  "id", upper(fiopro_cleared) fiopro_cleared,
			date_trunc('month', date_sale) period_sale,
			channel, filial,
			count(1) sale
	from srs
	group by "id", channel, filial, upper(fiopro_cleared),date_trunc('month', date_sale)
),
agent_sales_tbl as (
	select r.*,
		row_number() over (partition by "id", fiopro_cleared order by "id",fiopro_cleared )-
		row_number() over (partition by ("id", fiopro_cleared, status) order by period_sale ) cons_period
	from (
-- собираем все продажи агента по всем периодам, если продаж не было - выводим null
		select gr.id, gr.fiopro_cleared, gr.period_sale, gr.min_date_sale, s.sale, s.channel, s.filial,
				row_number() over () as row_id,					
				(case when sale is null then false else true end) status
		from grid_tbl gr
		full join (
-- 	собираем факт продаж по периодам и агентам
				select  "id", upper(fiopro_cleared) fiopro_cleared,channel, filial,
						date_trunc('month', date_sale) period_sale,						
						count(1) sale
				from srs
				group by "id", channel, filial, upper(fiopro_cleared),date_trunc('month', date_sale)
			) s using(id, period_sale)
		) r
),
agent_sale_cons_period_prep_tbl as (
	select	"id",
			fiopro_cleared,
			channel,
			filial,
			period_sale,
			min_date_sale,
			sale,
			status,
			row_number() over (partition by "id", fiopro_cleared order by "id",fiopro_cleared )-
			row_number() over (
				partition by ("id", fiopro_cleared, status) order by period_sale ) cons_period
			
from agent_sales_tbl
),*/
/*
agent_sale_by_serv as (
	select "id", fiopro_cleared, period_sale, 
			string_agg(distinct channel, chr(10) order by channel),
			sum("IPTV") as iptv_fact , sum("СОП") as ota_fact,
			sum("ЦТВ") as ktv_fact, sum("ШПД") as spd_fact
	from crosstab(
		$$ select dense_rank() over (order by "id", fiopro_cleared,
								 period_sale)::int row_name,
				"id", fiopro_cleared, period_sale, channel, serv_name, sale

		from (select "id", upper(fiopro_cleared) fiopro_cleared,
						date_trunc('month', date_sale) period_sale,	
			  			serv_name, channel,	 		
				 		count(1)::int sale 
				from newsales
				group by "id", upper(fiopro_cleared), 
						date_trunc('month', date_sale), 
			  			serv_name, channel
--			 	having channel ='Активный канал' and 
--			  	date_trunc('month', date_sale) between '2019-01-01'::timestamp and '2019-04-01'::timestamp
			 	) ct
		order by fiopro_cleared, period_sale, serv_name $$,
		$$ select distinct serv_name from newsales order by 1$$
		) tt (row_name int, "id" int, fiopro_cleared text, period_sale timestamp, channel text,
		  "IPTV" int, "СОП" int, "ЦТВ" int, "ШПД" int)
	group by "id", fiopro_cleared, period_sale 
)*/

select 	count(1) over (
		partition by (ag.fiopro_cleared, ag.filial)	order by ag.period_sale ) period_count_total,
		count(1) over (
		partition by (ag.fiopro_cleared, ag.filial, ag.status, cons_period_group) order by ag.period_sale ) cons_period,
		cons_period_group,
		ag.fiopro_cleared,
		ag.period_sale ag_period_sale,
		ag.status ag_status,
		min_date_sale, channel, filial,
		(case when  spd_fact is not null and spd_fact >= 22
					and iptv_fact+ktv_fact is not null and iptv_fact+ktv_fact >=19
					and ota_fact is not null and ota_fact>=5
		 then true else false end) as min_sale_check,
		(case when spd_fact >= spd and iptv_fact >= iptv and ota_fact >= ota 
				and spd_fact is not null 
				and iptv_fact is not null 
				and ota_fact is not null then true
		 else false end) as plan_sale_check,
		sale total_sale,
		spd_fact, iptv_fact,ktv_fact, ota_fact,
		first_value(ag.period_sale) over (
		partition by (ag.fiopro_cleared,ag.filial, ag.status, cons_period_group)
		order by ag.period_sale ) first_cons_period,
		tt.*
-- продажи итого по агентам для всех периодов
from (
	select r.*,
		row_number() over (partition by fiopro_cleared, filial order by period_sale )-
		row_number() over (partition by (fiopro_cleared, filial, status) order by period_sale ) cons_period_group
	from (
-- собираем все продажи агента по всем периодам, если продаж не было - выводим null
		select gr.fiopro_cleared, gr.period_sale, gr.min_date_sale, s.sale, gr.channel, gr.filial,
				row_number() over () as row_id,					
				(case when sale is null then false else true end) status
		from grid_tbl gr
		left join (
-- 	собираем факт продаж по периодам и агентам
				select  upper(fiopro_cleared) fiopro_cleared, filial,
						date_trunc('month', date_sale) period_sale,						
						count(1) sale
				from newsales
				group by filial, upper(fiopro_cleared),date_trunc('month', date_sale)
		) s using(fiopro_cleared, period_sale, filial )
	) r	
) ag
-- подтягиваем разбивку продаж по услугам
full join (
		select  fiopro_cleared, period_sale, filial,
				sum("IPTV") as iptv_fact , sum("СОП") as ota_fact,
				sum("ЦТВ") as ktv_fact, sum("ШПД") as spd_fact

		from crosstab(
			-- 1й аргумент crosstab
			$$ select dense_rank() over (order by fiopro_cleared, filial, period_sale)::int row_name,
						fiopro_cleared, period_sale, filial, serv_name, sale

			from (
					select  upper(fiopro_cleared) fiopro_cleared,
							date_trunc('month', date_sale) period_sale,	
							serv_name, filial,	 		
							count(1)::int sale 
					from newsales
					group by upper(fiopro_cleared), filial, date_trunc('month', date_sale),serv_name
	--			 	having channel ='Активный канал' and 
	--			  	date_trunc('month', date_sale) between '2019-01-01'::timestamp and '2019-04-01'::timestamp
					) s
			order by fiopro_cleared, period_sale, serv_name $$,

			-- 2й аргумент crosstab				
			$$ select distinct serv_name from newsales order by 1$$
		)	tt (row_name int, fiopro_cleared text, period_sale timestamp, filial text,
			  "IPTV" int, "СОП" int, "ЦТВ" int, "ШПД" int)
		group by fiopro_cleared, period_sale, filial 
) ct using(fiopro_cleared, period_sale, filial)
-- подтягиваем план продаж
full join agent_plan tt on ag.fiopro_cleared=upper(tt.fiopro) and ag.period_sale=tt.period_sale
--where  fiopro_cleared ='ОРЛОВА ТАТЬЯНА ВИКТОРОВНА'--ag.period_sale between '2019-01-01'::timestamp and '2019-04-01'::timestamp

	--offset 9
order by fiopro_cleared, ag_period_sale, first_cons_period