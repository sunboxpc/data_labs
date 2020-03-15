--select * from crs.cso_connects where period in (201912);
--select * from crs.cso_money_basic_services 
--where period_connect between 201908 and 201910 AND SUBS_ID = 10287195
--FETCH FIRST 5 ROWS ONLY
/*
SELECT * 
FROM ( 
		SELECT s.*, ROW_NUMBER() OVER (partition by CLIE_ID, SUBS_ID order by PERIOD_SERV asc) as rk
		FROM crs.cso_money_basic_services  s 
	) 
WHERE rk = 1
FETCH FIRST 100 ROWS ONLY


SELECT DISTINCT PERIOD_SERV
from crs.cso_money_basic_services 
FETCH FIRST 100 ROWS ONLY*/

select *
from (
	select PERIOD_CONNECT, CLIE_ID, SUBS_ID, PERIOD_SERV, SUMM_SERV as AMOUNT_SERV,
	row_number() over(partition by CLIE_ID, SUBS_ID order by PERIOD_SERV) as PERIOD_SERV_ID
	from crs.cso_money_basic_services
	where PERIOD_CONNECT between 202001 and 202002 and (PERIOD_SERV - PERIOD_CONNECT) between 0 and 4
	and rownum <=100
	order by PERIOD_CONNECT, CLIE_ID, SUBS_ID, PERIOD_SERV
	)
pivot ( sum(AMOUNT_SERV), max(PERIOD_SERV) 
		for PERIOD_SERV_ID in (1 as P1,2 as P2,3 as P3,4 as P4)--1 as P1_SERV, 2 as P2_SERV, 3 as P3_SERV, 4 as P4 SERV)
	)
