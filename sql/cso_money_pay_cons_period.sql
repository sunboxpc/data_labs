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
    where CLIE_ID = 41196163
 
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

order by CLIE_ID desc, PERIOD_CONNECT    
--    FETCH NEXT 100 ROWS ONLY    
 --   group by PERIOD_CONNECT, CLIE_ID, PERIOD_PAY, AMOUNT_PAY 
