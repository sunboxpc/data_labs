/*select  date(date_trunc('month',date_sale)), sales_date, to_date(to_char(d.period, '999999'), 'YYYYMM'), d.period
from debt as d
full outer join newsales as s on s.clie_id = d.user_id
where to_date(to_char(d.period, '999999'), 'YYYYMM') >= date(date_trunc('month',date_sale)) --s.clie_id is null--d.user_id is null
limit 5	*/

with tmp as (
	select *, 
	row_number() over (partition by user_id 
						   order by period)
						   as rk
	from debt
	where to_date(to_char(period, '999999'), 'YYYYMM') >= sale_period
)

select user_id, sale_period, period, sum_debt, sum_overdue_deb, total_deb, rk,
		(case when user_id in (select clie_id from newsales where fiopro_cleared like 'КАТАЕВ ИГОРЬ%') then 1
		else 0
		end) as kat
from tmp
where user_id in (select distinct clie_id from newsales where filial like '%Пермский%') --rk < 4 and 
order by user_id, period, rk
--limit 5