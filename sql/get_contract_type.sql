select distinct t12.contract_type
from (
	select distinct t2.contract_type as contract_type, UPPER(TRIM(t1.fio)) as fiopro 
	from agent as t1
	join agent2 as t2 on t1.code = t2.code
	where vedomost like '015%'
	) as t12
left join (
	select distinct fiopro, "key" 
	from scource) as s on t12.fiopro = s.fiopro
where s.key is not null
