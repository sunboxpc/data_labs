--alter table temp add column md5 uuid unique
/*
select t1.column_name as temp, t1.data_type as temp_type
from (
	select *
	from information_schema.columns
	where table_name = 'temp'
	) as t1
*/
--alter table churn drop column period
--update temp set md5 = cast(md5(cast(temp.* as text)) as uuid)

/*
-- drop duplicated churns and stay only with min date churn
delete from temp 
where (nz, date_churn) in (select nz , max_date 
			 from 
			 	(select  clie_id, subs_id, nz, count(1), max(date_churn) as max_date			 
				from temp
				group by clie_id, subs_id, nz
				having count(1) > 1
				--order by count(1) desc
				 ) as foo) 
-- order by nz
*/


--alter table temp rename column date_sale to date_churn
/*
alter table temp 
drop column index, 
drop column np,
drop column street,
drop column  house,
drop column korp,
drop column apartment,
drop column account,
drop column ddate
*/


