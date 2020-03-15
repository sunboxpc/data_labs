select 	CLIE_ID, SUBS_ID, ACCOUNT, NZ, CHANNEL, CHANNEL_DETAIL, UPPER(FIOPRO) as FIOPRO, UPPER(FIO_SUP) as FIO_SUP, 
		DATE_SALE, PERIOD as PERIOD_SALE, SER_NAME, RTPL_ID, TP, BRNC_ID, FILIAL, LTC, TSET, TOP147, 
		TOWN_ID, NP, STREET, HOUSE, KORP, APARTMENT, PHONE_ROOM		
from crs.cso_connects as s 
where PERIOD between 202001 and 202002
	and rownum<10
--left join () as c using (CLIE_ID, SUBS_ID)
left join (
	select PERIOD_CONNECT, CLIE_ID, SUBS_ID, P1_PERIOD_SERV, P1_AMOUNT_SERV, P2_PERIOD_SERV, P2_AMOUNT_SERV,
	        P3_PERIOD_SERV, P3_AMOUNT_SERV,P4_PERIOD_SERV, P4_AMOUNT_SERV      
	from (
		select PERIOD_CONNECT, CLIE_ID, SUBS_ID, PERIOD_SERV, SUMM_SERV as AMOUNT_SERV,
		row_number() over(partition by CLIE_ID, SUBS_ID order by PERIOD_SERV) as PERIOD_SERV_ID
		from crs.cso_money_basic_services
		where PERIOD_CONNECT between 202001 and 202012 and (PERIOD_SERV - PERIOD_CONNECT) between 0 and 4
	--	and rownum <=100
		) 
	pivot ( sum(AMOUNT_SERV) as AMOUNT_SERV, max(PERIOD_SERV) as PERIOD_SERV
			for PERIOD_SERV_ID in (1 as P1,2 as P2,3 as P3,4 as P4)
			)
	) as s using (CLIE_ID, SUBS_ID)
left join (
		select 	PERIOD_CONNECT, CLIE_ID, P1_PERIOD_EQ, P1_AMOUNT_EQ, P2_PERIOD_EQ, P2_AMOUNT_EQ,
	        P3_PERIOD_EQ, P3_AMOUNT_EQ,P4_PERIOD_EQ, P4_AMOUNT_EQ      
	from (
		select PERIOD_CONNECT, CLIE_ID, PERIOD_EQ, SUM_EQ,
		row_number() over(partition by CLIE_ID order by PERIOD_EQ) as PERIOD_EQ_ID
		from crs.cso_money_equipments 
		where PERIOD_CONNECT between 202001 and 202012 and (PERIOD_EQ - PERIOD_CONNECT) between 0 and 4
	--	and rownum <10
		)
	pivot ( sum(SUM_EQ) as AMOUNT_EQ, max(PERIOD_EQ) as PERIOD_EQ
			for PERIOD_EQ_ID in (1 as P1,2 as P2,3 as P3,4 as P4)
		)	
	) as e using (CLIE_ID)
left join (
	select 	PERIOD_CONNECT, CLIE_ID, P1_PERIOD_PAY, P1_AMOUNT_PAY, P2_PERIOD_PAY, P2_AMOUNT_PAY,
			P3_PERIOD_PAY, P3_AMOUNT_PAY, P4_PERIOD_PAY, P4_AMOUNT_PAY
	from (
		select PERIOD_CONNECT, CLIE_ID, PERIOD_PAY, AMOUNT_PAY, 
		row_number() over(partition by CLIE_ID order by PERIOD_PAY) as PERIOD_PAY_ID
		from (
			select PERIOD_CONNECT, CLIE_ID, TO_NUMBER(TO_CHAR(DATE_PAY, 'YYYYMM')) as PERIOD_PAY, SUM(SUM_PAY) as AMOUNT_PAY
			from crs.cso_money_pay
			group by PERIOD_CONNECT, CLIE_ID, TO_NUMBER(TO_CHAR(DATE_PAY, 'YYYYMM'))
			having PERIOD_CONNECT between 202001 and 202012 
			--	and DATE_PAY between TO_TIMESTAMP('01-12-2019 00:00:00') and TO_TIMESTAMP('01-03-2020 00:00:00')
			--	and rownum <100
			)
		)
	pivot ( sum(AMOUNT_PAY) as AMOUNT_PAY, max(PERIOD_PAY) as PERIOD_PAY
			for PERIOD_PAY_ID in (1 as P1,2 as P2,3 as P3,4 as P4)
			)
	) as p using (CLIE_ID)