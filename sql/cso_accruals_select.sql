select PERIOD_CONNECT, CLIE_ID, SUBS_ID, P1_PERIOD_SERV, P1_AMOUNT_SERV, P2_PERIOD_SERV, P2_AMOUNT_SERV,
        P3_PERIOD_SERV, P3_AMOUNT_SERV,P4_PERIOD_SERV, P4_AMOUNT_SERV      
from (
	select PERIOD_CONNECT, CLIE_ID, SUBS_ID, PERIOD_SERV, SUMM_SERV as AMOUNT_SERV,
	row_number() over(partition by CLIE_ID, SUBS_ID order by PERIOD_SERV) as PERIOD_SERV_ID
	from crs.cso_money_basic_services
	where PERIOD_CONNECT between 202001 and 202002 and (PERIOD_SERV - PERIOD_CONNECT) between 0 and 4
--	and rownum <=100

	) 
pivot ( sum(AMOUNT_SERV) as AMOUNT_SERV, max(PERIOD_SERV) as PERIOD_SERV
		for PERIOD_SERV_ID in (1 as P1,2 as P2,3 as P3,4 as P4)--1 as P1_SERV, 2 as P2_SERV, 3 as P3_SERV, 4 as P4 SERV)
	)
order by PERIOD_CONNECT desc, CLIE_ID, SUBS_ID