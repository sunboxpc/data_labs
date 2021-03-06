select 	CLIE_ID, SUBS_ID, ACCOUNT, NZ, CHANNEL, CHANNEL_DETAIL, UPPER(FIOPRO) as FIOPRO, UPPER(FIO_SUP) as FIO_SUP, 
		DATE_SALE, to_date(to_char(PERIOD), 'YYYYMM') as PERIOD_SALE,SERV_NAME, RTPL_ID, TP, BRNC_ID, FILIAL, LTC, TOP147, --TCET as TSTET,
		TOWN_ID, NP, STREET, HOUSE, KORP, APARTMENT, PHONE_ROOM, DATE_CHURN
        --, trunc(DATE_CHURN, 'YYYYMM') as PERIOD_CHURN
        , NAME_GRP, 1 as NEWSALE,
		case when (DATE_CHURN is null)  then 0 else 1 end as CHURN, case when P1_AMOUNT_PAY > 0 then 1 else 0 end as PAYSTATUS, 
		nvl(P1_PERIOD_SERV,0) as "P1_period_serv", nvl(P1_AMOUNT_SERV,0) as "P1_amount_serv", nvl(P2_PERIOD_SERV,0) as "P2_period_serv",
		nvl(P2_AMOUNT_SERV,0) as "P2_amount_serv", nvl(P3_PERIOD_SERV,0) as "P3_period_serv", nvl(P3_AMOUNT_SERV,0) as "P3_amount_serv",
		nvl(P4_PERIOD_SERV,0) as "P4_period_serv", nvl(P4_AMOUNT_SERV,0) as "P4_amount_serv", 
        nvl(P1_PERIOD_EQ,0) as "P1_period_eq", nvl(P1_AMOUNT_EQ,0) as "P1_amount_eq", nvl(P2_PERIOD_EQ,0) as "P2_period_eq",
        nvl(P2_AMOUNT_EQ,0) as "P2_amount_eq", nvl(P3_PERIOD_EQ,0) as "P3_period_eq", nvl(P3_AMOUNT_EQ,0) as "P3_amount_eq",
		nvl(P4_PERIOD_EQ,0) as "P4_period_eq", nvl(P4_AMOUNT_EQ,0) as "P4_amount_eq",
        nvl(P1_PERIOD_PAY,0) as "P1_period_pay", nvl(P1_AMOUNT_PAY,0) as "P1_amount_pay", nvl(P2_PERIOD_PAY,0) as "P2_period_pay", 
        nvl(P2_AMOUNT_PAY,0) as "P2_amount_pay", nvl(P3_PERIOD_PAY,0) as "P3_period_pay", nvl(P3_AMOUNT_PAY,0) as "P3_amount_pay", 
        nvl(P4_PERIOD_PAY,0) as "P4_period_pay", nvl(P4_AMOUNT_PAY,0) as "P4_amount_pay"
        , nvl(round((P1_AMOUNT_PAY+P2_AMOUNT_PAY+P3_AMOUNT_PAY)/3,2),0) as AVG_PAY_3M
        , nvl(round((P1_AMOUNT_SERV+P2_AMOUNT_SERV+P3_AMOUNT_SERV)/3,2),0) as AVG_SERV_3M
        , nvl(round((P1_AMOUNT_EQ+P2_AMOUNT_EQ+P3_AMOUNT_EQ)/3,2),0) as AVG_EQ_3M
--       ,  as MAX_DATE
        , case when (DATE_CHURN is null) 
            then first_value(DATE_SALE) over(partition by 1 order by DATE_SALE desc)-DATE_SALE
            else TO_NUMBER(DATE_CHURN-DATE_SALE) end as LIFE_TIME_script
--        , ORA_HASH(nvl(FILIAL,'') || nvl(UPPER(FIOPRO),'') || nvl(CHANNEL,''), 1e8) as KEY 
 
from (
    select CLIE_ID, SUBS_ID, ACCOUNT, NZ, CHANNEL, CHANNEL_DETAIL, FIOPRO, FIO_SUP, 
		DATE_SALE, PERIOD, SERV_NAME, RTPL_ID, TP, BRNC_ID, FILIAL, LTC, TOP147, --TCET as TSTET,
		TOWN_ID, NP, STREET, HOUSE, KORP, APARTMENT, PHONE_ROOM    
    from crs.cso_connects t
    where t.PERIOD between 202001 and 202003 and CHANNEL = '�������� �����'
    )
left join (
    select CLIE_ID, SUBS_ID, NAME_GRP, DATE_CHURN
    from crs.cso_disconnects 
    where exists (	
        select * 
        from crs.cso_connects
		where crs.cso_connects.SUBS_ID = crs.cso_disconnects.SUBS_ID
                and crs.cso_connects.CLIE_ID = crs.cso_disconnects.CLIE_ID
                and  crs.cso_connects.DATE_SALE <= crs.cso_disconnects.DATE_CHURN
				) 
            and PERIOD between 202001 and 202012
    ) c using (CLIE_ID, SUBS_ID)
left join (
	select PERIOD_CONNECT, CLIE_ID, SUBS_ID, nvl(P1_PERIOD_SERV,0) as P1_PERIOD_SERV
        , nvl(P1_AMOUNT_SERV,0) as P1_AMOUNT_SERV, nvl(P2_PERIOD_SERV,0) as P2_PERIOD_SERV
        , nvl(P2_AMOUNT_SERV,0) as P2_AMOUNT_SERV, nvl(P3_PERIOD_SERV,0) as P3_PERIOD_SERV
        , nvl(P3_AMOUNT_SERV,0) as P3_AMOUNT_SERV,nvl(P4_PERIOD_SERV,0) as P4_PERIOD_SERV
        , nvl(P4_AMOUNT_SERV,0) as P4_AMOUNT_SERV     
	from (
		select PERIOD_CONNECT, CLIE_ID, SUBS_ID, PERIOD_SERV, SUMM_SERV as AMOUNT_SERV,
		row_number() over(partition by CLIE_ID, SUBS_ID order by PERIOD_SERV) as PERIOD_SERV_ID
		from crs.cso_money_basic_services
		where PERIOD_CONNECT between 202001 and 202012 and (PERIOD_SERV - PERIOD_CONNECT-1) between 0 and 3
	--	and rownum <=100
		) 
	pivot ( sum(AMOUNT_SERV) as AMOUNT_SERV, max(PERIOD_SERV) as PERIOD_SERV
			for PERIOD_SERV_ID in (1 as P1,2 as P2,3 as P3,4 as P4)
			)
	) s using (CLIE_ID, SUBS_ID)
left join (
		select 	PERIOD_CONNECT, CLIE_ID, nvl(P1_PERIOD_EQ,0) as P1_PERIOD_EQ
        , nvl(P1_AMOUNT_EQ,0) as P1_AMOUNT_EQ, nvl(P2_PERIOD_EQ,0) as P2_PERIOD_EQ
        , nvl(P2_AMOUNT_EQ,0) as P2_AMOUNT_EQ, nvl(P3_PERIOD_EQ,0) as P3_PERIOD_EQ
        , nvl(P3_AMOUNT_EQ,0) as P3_AMOUNT_EQ,nvl(P4_PERIOD_EQ,0) as P4_PERIOD_EQ
        , nvl(P4_AMOUNT_EQ,0) as P4_AMOUNT_EQ       
	from (
		select PERIOD_CONNECT, CLIE_ID, PERIOD_EQ, SUM_EQ,
		row_number() over(partition by CLIE_ID order by PERIOD_EQ) as PERIOD_EQ_ID
		from crs.cso_money_equipments 
		where PERIOD_CONNECT between 202001 and 202012 and (PERIOD_EQ - PERIOD_CONNECT -1 ) between 0 and 3
	--	and rownum <10
		)
	pivot ( sum(SUM_EQ) as AMOUNT_EQ, max(PERIOD_EQ) as PERIOD_EQ
			for PERIOD_EQ_ID in (1 as P1,2 as P2,3 as P3,4 as P4)
		)	
	) e using (CLIE_ID)
left join (
select  CLIE_ID, nvl(P1_PERIOD_PAY,0) as P1_PERIOD_PAY 
        , nvl(P1_AMOUNT_PAY, 0) as P1_AMOUNT_PAY, nvl(P2_PERIOD_PAY,0) as P2_PERIOD_PAY
        , nvl(P2_AMOUNT_PAY,0) as P2_AMOUNT_PAY, nvl(P3_PERIOD_PAY,0) as P3_PERIOD_PAY
        , nvl(P3_AMOUNT_PAY,0) as P3_AMOUNT_PAY, nvl(P4_PERIOD_PAY,0) as P4_PERIOD_PAY
        , nvl(P4_AMOUNT_PAY,0) as P4_AMOUNT_PAY
--        , first_value(PERIOD_CONNECT) over (partition by CLIE_ID order by PERIOD_CONNCET) as PERIOD_CONNECT
        , PERIOD_CONNECT
--        , PAY_ID
from (
	select PERIOD_CONNECT, CLIE_ID, PERIOD_PAY, AMOUNT_PAY 
        , row_number() over(partition by CLIE_ID order by PERIOD_PAY) as PERIOD_PAY_ID
--        , PAY_ID
--      ����� ��� ��������� ������� ������� ������ � Pivot table
/*    , case when 
        lag(PERIOD_PAY, 1, 0) over(partition by CLIE_ID order by PERIOD_PAY) = 0 then 1
        else 
        PERIOD_PAY - lag(PERIOD_PAY, 1, 0) over (partition by CLIE_ID order by PERIOD_PAY) +1
        end as PERIOD_PAY_ID
 */ 
        , PERIOD_PAY - row_number() over (partition by CLIE_ID order by PERIOD_PAY) + 1 as CONS_PERIOD_PAY_GRP 
/*      There�s a simple example for this behavior:
        1. ROW_NUMBER() never has gaps. That�s how it�s defined
        2. Our data, however, does
        So when we subtract a �gapless� series of consecutive integers from a �gapful� series of non-consecutive dates, 
        we will get the same date for each �gapless� subseries of consecutive dates, 
        and we�ll get a new date again where the date series had gaps
        https://jaxenter.com/10-sql-tricks-that-you-didnt-think-were-possible-125934.html
*/
        , min(PERIOD_PAY) over (partition by CLIE_ID order by PERIOD_PAY) as MIN_PERIOD_PAY
/*      ����������� ������ ��� ���� �������� ������� (� ������ ������� where ����) 
        ����� ��� ����������� ������ ������ ������ �������� (������ n ���������������� �������� � ������� �����������)  
*/
 --       , max(row_number()) over (partition by CLIE_ID order by PERIOD_PAY) as PAY_COUNT -- ������� ��������������� CLIE_ID � ����������� ���������
	from (    
		select CLIE_ID, PERIOD_PAY, SUM(SUM_PAY) as AMOUNT_PAY
            ,PERIOD_CONNECT 
--            , max(PERIOD_CONNECT) as PERIOD_CONNECT, PAY_ID
		from ( 
            select CLIE_ID, SUM_PAY, PERIOD_CONNECT
                    , TO_NUMBER(TO_CHAR(DATE_PAY, 'YYYYMM')) as PERIOD_PAY    
--                    , row_number() over (
--                    partition by CLIE_ID, DATE_PAY, SUM_PAY
--                        order by PERIOD_CONNECT desc) as PAY_ID
            from crs.cso_money_pay
            where exists (
                select CLIE_ID
                from crs.cso_connects
                where crs.cso_connects.CLIE_ID = crs.cso_money_pay.CLIE_ID
                and  crs.cso_connects.CHANNEL = '�������� �����'
                and  crs.cso_connects.DATE_SALE <= crs.cso_money_pay.DATE_PAY
                and  TO_NUMBER(TO_CHAR(DATE_PAY, 'YYYYMM'))-PERIOD_CONNECT-1 between 0 and 3
                )
            )    
		group by CLIE_ID, PERIOD_PAY
                , PERIOD_CONNECT
 --               , PAY_ID
--		having 
--            PERIOD_CONNECT between 202001 and 202003
--            and TO_NUMBER(TO_CHAR(DATE_PAY, 'YYYYMM')) >=PERIOD_CONNECT
--            and 
--              PAY_ID = 1
                   
		)
--    where CLIE_ID = 41196163
 
)
	pivot ( sum(AMOUNT_PAY) as AMOUNT_PAY, max(PERIOD_PAY) as PERIOD_PAY
			for PERIOD_PAY_ID in (1 as P1,2 as P2,3 as P3,4 as P4)
			)
            
where -- MIN_PERIOD_PAY - CONS_PERIOD_PAY_GRP = 0 --������� ������ ������ ������ ���������������� ��������
/*      CONS_PERIOD_PAY_GRP ����� ���������� �������� ��� CLIE_ID � �������� ��������
        ������� ���� ��������������� ���� �� ������ ��� ���������
        � ������� ��� ���������������� �������� �������� 202001, 202002, 202003 
        �������� CONS_PERIOD_PAY_GRP ����� ����� 202001. ���������� ��������� (��������� 1), ����� 
        ������ �������� ��������� � ������ (�����������) �������� �������, ��� �������� �������� ��� ������
        ����� ��������. ������ ��������� ������ ���������������� �������� �������������, �.�. 
        ��� ������� ����� ������ ������ n ���������������� ��������.
        � �������, ���� ����� ������ ���������������� �������� 202001, 200202, 202003
        ����� ���� ������������������ 202005, 202007, 202008, 202009 (� ������� 202004 � 202006 �� ���� ��������), 
        �� ��� 202005 CONS_PERIOD_PAY_GRP ����� ����� 202002 (��� �� ����� ������� �������), � ��� ������������������ 
        202007, 202008, 202009 �������� ����� 202003 (202007 ���� 6 �� ������� � ������ ��������, ����� 202007-6+1=202003)
        ������������� �����, ��� CONS_PERIOD_PAY_GRP �� ������ �� ���� ��������, � ���� ����������� ���������,
        ������� ���������� �����-�� ������ ��������, ��������� � ������ �������� ������� �������� ���� ���������
        ������ ������ ������ ���������������� ��������       
*/    
        PERIOD_CONNECT - MIN_PERIOD_PAY < 4 -- ������� ������ ����� ������� �� 3 ������ � ������� �����������
/*      ��� ������� ������������ ��� ������� ������ ����, ������� ����� ������� � ������ 3 �������.
        ��� ���� ���� ������ ��������� 202001, � ������ 202001, 202003, 202004, �� ��������� �������
        202001 (P1*) � 202003 (P2*). ����� 202003 ������� ��� P2 �� ���������� ��������, � �� �� ����������� ������������������
        �������� 202001, 202002, 202003.
        ����� ���� ���� ������������� ������ ��������, ��������� � ������������ ������������ PERIOD_CONNECT.
        ���� PERIOD_CONNECT ������������ �� ������ ���� ����������� �������, �� ��� ����������� �����������
        ����� ��������� ������� � ������� ������� �����������. � �������, ���� ������ ������� ��������� 202001, � �����
        ���� ��� ����������� 202004 � 202010, �� ��� ���� ����������� ����� �������� ������� �� 202001, 202002 � 202003
        �������� ������ �������� ����� ���� ������ Pivot table ��� ������� ������� ����������� ����� �������, �.�.
        ����� �������� ������� ������� ���������� ��� ����������� ������� � ������� ��� ������� �� ���� ��������� �����������,
        ��������, ��� ��� ������� ����������� ����� �� ����� 3 ������� � ������� ����������� � ���� ����� �������������
        ������ ������ 3 �������� (���� �� ������-������ ���� �� ������), �� ����� ������� �� ���������� �����������
*/

) p using (CLIE_ID)
order by DATE_SALE
--FETCH NEXT 100 ROWS ONLY
 