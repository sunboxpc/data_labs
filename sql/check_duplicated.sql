select *
from temp 
where nz in (select nz  
			 from 
			 	(select clie_id, subs_id, nz, count(1)			 
				from churn
				group by clie_id, subs_id, nz
				having count(1) > 1
				--order by count(1) desc
				 ) as foo) 
 order by nz