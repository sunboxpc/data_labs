select 	PERIOD_CONNECT, CLIE_ID, P1_PERIOD_EQ, P1_AMOUNT_EQ, P2_PERIOD_EQ, P2_AMOUNT_EQ,
        P3_PERIOD_EQ, P3_AMOUNT_EQ,P4_PERIOD_EQ, P4_AMOUNT_EQ      
from (
	select PERIOD_CONNECT, CLIE_ID, PERIOD_EQ, SUM_EQ,
	row_number() over(partition by CLIE_ID order by PERIOD_EQ) as PERIOD_EQ_ID
	from crs.cso_money_equipments 
	where PERIOD_CONNECT between 201912 and 202012 and (PERIOD_EQ - PERIOD_CONNECT) between 0 and 4
--	and rownum <10
	)
pivot ( sum(SUM_EQ) as AMOUNT_EQ, max(PERIOD_EQ) as PERIOD_EQ
		for PERIOD_EQ_ID in (1 as P1,2 as P2,3 as P3,4 as P4)
	)
order by PERIOD_CONNECT desc, CLIE_ID