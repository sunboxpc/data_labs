with temp as (
	select distinct concat(clie_id, subs_id, nz) as key, clie_id, subs_id, nz, count(1)
from newsales
group by clie_id, subs_id, nz
having count(1)>1--clie_id = 39786623
--where clie_id = 39786623 
  )
  
select * 
from newsales
where concat(clie_id , subs_id, nz) in (select key from temp) 

--alter table scource add column dublicated integer
--update scource set dublicated = 1 where clie_id = 39786623 and date_churn = (select max(date_churn) from scource where clie_id=39786623)
