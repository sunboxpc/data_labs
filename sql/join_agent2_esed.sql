/*select distinct count(t2.contract_name)--, count(t1.reg_num)
from agent2 as t2
left join 
	(select distinct reg_num from esed) as t1
	on t2.contract_name = t1.reg_num
where t1.reg_num is not NULL
*/

select distinct t1.contract_details--, count(t1.reg_num)
from agent2 as t2
left join 
	(select distinct contract_details, reg_num from esed) as t1
	on t2.fio = t1.contract_details and t2.contract_name = t1.reg_num
where t1.contract_details is NULL