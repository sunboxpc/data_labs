CREATE OR REPLACE FUNCTION To_Timestamp_From_Excel (ExcelDate integer)
RETURNS timestamp without time zone AS $$
BEGIN
   IF ExcelDate > 59 THEN
    ExcelDate = ExcelDate - 1;
   END IF;
   RETURN date '1899-12-31' + ExcelDate;
END;
$$ LANGUAGE plpgsql;

with agent2 as (
  select
    *,
    row_number() over (partition by code order by "index" desc) as rk
  from agent2

),
-- выбираем такие ключи агентов, у которых были максимальные продажи в каждом периоде
fio_id as (
select distinct fiopro, "key", channel_detail, period_sale,sale,
	row_number() over (partition by fiopro, period_sale  order by sale desc) as rk
from (
		select fiopro, "key", channel_detail, period_sale, sum(newsale) as sale
			
	from scource	
	group by fiopro, "key", channel_detail, period_sale

) as foo
	)

--select distinct code, key
--from (
select distinct t1.code, t1.fiopro,
				f.key as key,
				t2.contract_type as contract_type,
				ct.channel_detail as channel_detail,
				t2.department_full_name as department_full_name,
				t2.department as department,
				t1.account,
				t1.expense_item,
				t1.element_name,
				t1.accrual_date,
				f.period_sale,
				t1.dt_sum,
				t1.cr_sum,
				f.sale
				
				
from (
	select	code as code,
			UPPER(TRIM(fio)) as fiopro,
			account,
			expense_item,
			element_name,
			sum(dt_sum) dt_sum,
			sum(cr_sum) cr_sum,
			date_trunc('month', 
						To_Timestamp_From_Excel(period:: integer)
					  )- interval '1 month' as accrual_date
--					ceiling (sum(t1.sum)) as paid
	from agent_pay
--	left join agent2 as t2 on t1.code = t2.code
	group by code, fiopro, account, expense_item,
			element_name, accrual_date
	) as t1
left join agent2 as t2 on t1.code = t2.code
left join channel_type as ct on ct.contract_type = t2.contract_type
left join fio_id as f on t1.fiopro = f.fiopro
--				and ct.channel_detail = f.channel_detail
				and t1.accrual_date = f.period_sale
where 	(f.rk = 1 
	or 	f.rk is null)
	and t2.rk = 1
	and (ct.channel_detail is not null
--			 or (ct.channel_detail is null and 
				 or t2.department_full_name like '%Группа активных продаж')
--	and f.fiopro ='РОМАНОВ ДМИТРИЙ ИГОРЕВИЧ'
--	and f.fiopro like 'СИДОРЕНКО ОЛЬГА%'
--	and f.fiopro like 'КАТАЕВ%'
order by t1.fiopro, t1.accrual_date, t1.account
/*left join (
	select distinct fiopro, 
					"key"
	from scource) as s 
	on t12.fiopro = s.fiopro
where ct.channel_detail is not null
*/
--where 	s.key is not null 
		--and 
		--ct.channel_detail = '' 
--		and ct.channel_detail is null 
--) as foo
