select t1.column_name as temp, t1.data_type as temp_type,
		t2.column_name as newsales, t2.data_type as newsales_type
from (
	select *
	from information_schema.columns
	where table_name = 'temp'
	) as t1
full join (
	select *
	from information_schema.columns
	where table_name = 'churn'
	) as t2 on t1.column_name = t2.column_name

