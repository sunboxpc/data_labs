CREATE OR REPLACE FUNCTION To_Timestamp_From_Excel (ExcelDate integer)
RETURNS timestamp without time zone AS $$
BEGIN
   IF ExcelDate > 59 THEN
    ExcelDate = ExcelDate - 1;
   END IF;
   RETURN date '1899-12-31' + ExcelDate;
END;
$$ LANGUAGE plpgsql;

with agent_calc as (
	select fiopro_cleared fiopro, period_sale as accrual_date, 
	string_agg(distinct fio_sup, chr(10) ORDER BY fio_sup) fio_sup,
		string_agg(distinct filial, chr(10)) as filial,
	ceiling(sum(total_agent_pay)) total_agent_pay,
	ceiling(sum(total_iptv_agent_pay)) total_iptv_agent_pay, ceiling(sum(total_spd_agent_pay)) total_spd_agent_pay, 
	ceiling(sum(total_ota_agent_pay)) total_ota_agent_pay
	from agent_pay_calc_upd
	group by fiopro_cleared, period_sale),

pay_r12 as (
	select 	split_part(code,'-',1) code,
			string_agg(distinct code, chr(10) ORDER BY code) as full_code_paid,
			UPPER(TRIM(fio)) 	as fiopro, 
			vedomost, 
			--to_char(
			date_trunc('month', To_Timestamp_From_Excel(prepaid_data:: integer))- interval '1 month'
			--,'YYYY-MON') 
			as accrual_date,
			ceiling (sum("sum")) as paid
	from agent
	group by accrual_date, fio, vedomost, accrual_date, split_part(code,'-',1)
	having vedomost like '015%' 
--		and  code in (select distinct code from agent2 where contract_type like '%ГПХ%')
	--order by date_trunc('month', 
	--						  To_Timestamp_From_Excel(prepaid_data:: integer))- interval '1 month' 
	),
	
pay_r12_serv as (
	select split_part(code,'-',1) code, fiopro, accrual_date, sum("3004070000") spd_agent_pay_fact,  
		sum("3004050000") iptv_agent_pay_fact,	sum("3004030000") ota_agent_pay_fact, 
		sum("3004080000") mvno_agent_pay_fact, sum("3004060000") ktv_agent_pay_fact,
		sum("3004590000") end_point_eq, sum("3004150500") smart_house_agent_pay_fact,
		sum("3004150400") cctv_smart_house_agent_pay_fact, sum("3004600000") other_eq_agent_pay_fact,
		sum("3004090000") web_ks_agent_pay_fact, sum("3004150100") cctv_b2b_agent_pay_fact 
from crosstab(
	$$ select dense_rank() over (order by code, fiopro, account, accrual_date)::int row_name,
		code, fiopro,  accrual_date, account, ceiling(sum(paid)) paid
	from (
		select code, UPPER(TRIM(fio)) fiopro, account, dt_sum paid,
				date_trunc('month', To_Timestamp_From_Excel(period:: integer))- interval '1 month'
				as accrual_date
		from agent_pay
		where code in (select code from agent where vedomost like '015%' 
/*					   and code in (
						   select distinct code 
						   from agent2 where contract_type like '%ГПХ%')*/
					   )
				and account like '3%') ct
	group by code, fiopro, account, accrual_date
	order by fiopro, accrual_date, account $$,
	$$ values	('3004030000'::text), ('3004050000'::text), ('3004060000'::text), 
	 			('3004070000'::text), ('3004080000'::text), ('3004090000'::text), 
				('3004150100'::text),('3004150400'::text), ('3004150500'::text),
	  			('3004590000'::text), ('3004600000'::text) $$) tt
	--$$ select distinct account from agent_pay 
	--	where code in (select code from agent where vedomost like '015%')
	--	and account like '3%'order by 1$$) tt
	(row_name int, code text, fiopro text, accrual_date timestamp,
	 			"3004030000" int, "3004050000" int, "3004060000" int, 
	 			"3004070000" int, "3004080000" int, "3004090000" int, 
	 			"3004150100" int, "3004150400" int, "3004150500" int, 
	 			"3004590000" int, "3004600000" int)
	group by split_part(code,'-',1), fiopro, accrual_date
)
, 
agent_profile as (
	select split_part(code,'-',1) code, upper(fio) fiopro,
			string_agg(distinct code || ' ' || contract_name || ' : ' || contract_start_data, 
					   chr(10)) contract_details,
			string_agg(distinct contract_type, 
					   chr(10)) contract_type,
			string_agg(distinct department, 
					   chr(10)) department,
			string_agg(distinct left(department_sys_name,5), 
					   chr(10))  filial_code,
			string_agg(distinct split_part(department_full_name, $$\$$,1), 
					   chr(10)) filial_name
	from agent2
	group by split_part(code,'-',1), fio
	having split_part(code,'-',1) in (select distinct code from pay_r12 )
),

grid_tbl as (
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
),

agent_sale as (
select 	count(1) over (
		partition by (ag.fiopro_cleared, ag.filial)	order by ag.period_sale ) period_count_total,
		count(1) over (
		partition by (ag.fiopro_cleared, ag.filial, ag.status, cons_period_group) order by ag.period_sale ) cons_period,
		cons_period_group,
		ag.fiopro_cleared as fiopro,
		ag.period_sale as accrual_date,
		ag.status as sale_status,
		ag.channel as channel_sale,
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
		tt.tstet, tt.fio_sup, tt.fio_sup_position, tt.contract_date, tt.status as fiopro_status,
		tt.fiopro_grade, tt.spd, tt.iptv, tt.ota, tt.mvno, tt.smart_house_cttv, tt.smart_house_base,
		tt.file
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
)

select  r.code code_key, r.fiopro, r.vedomost, r.accrual_date, r.paid, r.full_code_paid, s.*, ap.*, rs.*, apc.*,
		rs.spd_agent_pay_fact+rs.iptv_agent_pay_fact+rs.ota_agent_pay_fact total_spd_iptv_ota_fact,
		(case when rs.spd_agent_pay_fact+rs.iptv_agent_pay_fact+rs.ota_agent_pay_fact-apc.total_agent_pay is not null
			then rs.spd_agent_pay_fact+rs.iptv_agent_pay_fact+rs.ota_agent_pay_fact-apc.total_agent_pay
			else rs.spd_agent_pay_fact+rs.iptv_agent_pay_fact+rs.ota_agent_pay_fact end) as diff_total_agent_pay,
		(case when rs.spd_agent_pay_fact-apc.total_spd_agent_pay is not null
		 	then rs.spd_agent_pay_fact-apc.total_spd_agent_pay
		 	else rs.spd_agent_pay_fact end) diff_spd_agent_pay,
		(case when rs.iptv_agent_pay_fact-apc.total_iptv_agent_pay is not null
			then rs.iptv_agent_pay_fact-apc.total_iptv_agent_pay
		 	else rs.iptv_agent_pay_fact end) as diff_iptv_agent_pay,
		(case when rs.ota_agent_pay_fact-apc.total_ota_agent_pay is not null
			then rs.ota_agent_pay_fact-apc.total_ota_agent_pay
		 	else rs.ota_agent_pay_fact end) as diff_ota_agent_pay		
--		,ceiling(r.paid-apc.total) diff
from pay_r12 r
full join agent_sale as s using(fiopro, accrual_date)
full join agent_calc apc using(fiopro, accrual_date)
--full join agent_pay_calc apc on apc.fiopro_cleared = r.fiopro and apc.month_conection = r.accrual_date
--where s.key is not null and r.accrual_date = '2019-01-01'::date
left join pay_r12_serv rs using(code, fiopro, accrual_date)
left join agent_profile as ap using(code, fiopro)
--where rs.spd+rs.iptv+rs.ota>0 and apc.total is null
--where contract_type like '%ГПХ%'
where s.channel like '%Активный%'
order by r.fiopro, r.accrual_date

