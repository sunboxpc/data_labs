select clie_id, date_pay, sum_pay, 
		(case when clie_id in (select clie_id from newsales where fiopro_cleared like 'КАТАЕВ ИГОРЬ%') then 1
		else 0
		end) as kat
from payments
where clie_id in (
	select distinct clie_id 
	from scource 
	where filial like '%Пермский%' and serv_name = 'ШПД' ) --rk < 4 and 
order by clie_id, date_pay, kat
--limit 5