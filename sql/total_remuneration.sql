select distinct --t1.code as code,
--					t2.contract_type as contract_type, 
--					UPPER(TRIM(t1.fio)) as fiopro,
--					t1.vedomost as vedomost,
					t2.department as department,
					t2.contract_type as contract_type,
					date_trunc('month', 
						To_Timestamp_From_Excel(prepaid_data:: integer)
					  )- interval '1 month' as accrual_date,
					ceiling (sum(t1.sum)) as paid
from agent as t1
left join agent2 as t2 on t1.code = t2.code
group by t1.code, accrual_date, department, t1.vedomost,t2.contract_type
having t1.vedomost like '015%' 