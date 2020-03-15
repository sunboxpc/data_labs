CREATE OR REPLACE FUNCTION To_Timestamp_From_Excel (ExcelDate integer)
RETURNS timestamp without time zone AS $$
BEGIN
   IF ExcelDate > 59 THEN
    ExcelDate = ExcelDate - 1;
   END IF;
   RETURN date '1899-12-31' + ExcelDate;
END;
$$ LANGUAGE plpgsql;

--select distinct contract_type
--from (
select distinct t12.code as code,
				s.key as key,
				t12.fiopro,
				t12.contract_type as contract_type,
				ct.channel_detail as channel_detail,
				t12.vedomost as vedomost,
				t12.department_full_name as department_full_name,
				t12.accrual_date as accrual_date,
				t12.paid as paid
from (
	select distinct t1.code as code,
					t2.contract_type as contract_type, 
					UPPER(TRIM(t1.fio)) as fiopro,
					t1.vedomost as vedomost,
					t2.department_full_name as department_full_name,
					date_trunc('month', 
						To_Timestamp_From_Excel(prepaid_data:: integer)
					  )- interval '1 month' as accrual_date,
					ceiling (sum(t1.sum)) as paid
	from agent as t1
	left join agent2 as t2 on t1.code = t2.code
	group by t1.code, t2.contract_type, accrual_date, fiopro, t2.department_full_name,t1.vedomost
	having t1.vedomost like '015%'	
	) as t12
left join channel_type as ct on ct.contract_type = t12.contract_type
left join (
	select distinct fiopro, 
					"key",
					channel_detail
	from scource) as s 
	on t12.fiopro = s.fiopro
		and ct.channel_detail = s.channel_detail
--where 	s.key is not null 
		--and 
		--ct.channel_detail = '' 
--		and ct.channel_detail is null 
--) as foo
