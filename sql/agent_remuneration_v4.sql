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
select distinct fiopro, "key", channel_detail, period_date_sale,sale,
	row_number() over (partition by fiopro, period_date_sale  order by sale desc) as rk
from (
		select fiopro, "key", channel_detail, period_date_sale, sum(newsale) as sale
			
	from scource	
	group by fiopro, "key", channel_detail, period_date_sale

) as foo
	)

--select distinct code, key
--from (
select distinct t1.code as code,
				f.key as key,
				t1.fiopro,
				t2.contract_type as contract_type,
				ct.channel_detail as channel_detail,
				t1.vedomost as vedomost,
				t2.department_full_name as department_full_name,
				t2.department as department,
				t1.accrual_date as accrual_date,
				t1.paid as paid
from (
	select distinct t1.code as code,
--					t2.contract_type as contract_type, 
					UPPER(TRIM(t1.fio)) as fiopro,
					t1.vedomost as vedomost,
--					t2.department_full_name as department_full_name,
					date_trunc('month', 
						To_Timestamp_From_Excel(prepaid_data:: integer)
					  )- interval '1 month' as accrual_date,
					ceiling (sum(t1.sum)) as paid
	from agent as t1
--	left join agent2 as t2 on t1.code = t2.code
	group by t1.code, accrual_date, fiopro, t1.vedomost
	having t1.vedomost like '015%' 
			--and UPPER(TRIM(t1.fio)) like 'КАТАЕВ ИГОРЬ%'
	) as t1
left join agent2 as t2 on t1.code = t2.code
left join channel_type as ct on ct.contract_type = t2.contract_type
left join fio_id as f on t1.fiopro = f.fiopro
--				and ct.channel_detail = f.channel_detail
				and t1.accrual_date = f.period_date_sale
where 	(f.rk = 1 
	or 	f.rk is null)
	and t2.rk = 1
	and (ct.channel_detail is not null
--			 or (ct.channel_detail is null and 
				 or t2.department_full_name like '%Группа активных продаж')
--	and f.fiopro ='РОМАНОВ ДМИТРИЙ ИГОРЕВИЧ'
--	and f.fiopro like 'СИДОРЕНКО ОЛЬГА%'
--	and f.fiopro like 'КАТАЕВ%'

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
