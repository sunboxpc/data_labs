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
--      нужен для нумерации каждого периода платеж в Pivot table
/*    , case when 
        lag(PERIOD_PAY, 1, 0) over(partition by CLIE_ID order by PERIOD_PAY) = 0 then 1
        else 
        PERIOD_PAY - lag(PERIOD_PAY, 1, 0) over (partition by CLIE_ID order by PERIOD_PAY) +1
        end as PERIOD_PAY_ID
 */ 
        , PERIOD_PAY - row_number() over (partition by CLIE_ID order by PERIOD_PAY) + 1 as CONS_PERIOD_PAY_GRP 
/*      There’s a simple example for this behavior:
        1. ROW_NUMBER() never has gaps. That’s how it’s defined
        2. Our data, however, does
        So when we subtract a “gapless” series of consecutive integers from a “gapful” series of non-consecutive dates, 
        we will get the same date for each “gapless” subseries of consecutive dates, 
        and we’ll get a new date again where the date series had gaps
        https://jaxenter.com/10-sql-tricks-that-you-didnt-think-were-possible-125934.html
*/
        , min(PERIOD_PAY) over (partition by CLIE_ID order by PERIOD_PAY) as MIN_PERIOD_PAY
/*      минимальный период для всех платежей клиента (с учетом фильтра where ниже) 
        нужен для дальнейшего отбора нужной группы платежей (первые n последовательных платежей с момента подключения)  
*/
 --       , max(row_number()) over (partition by CLIE_ID order by PERIOD_PAY) as PAY_COUNT -- попытка отфильтровывать CLIE_ID с несколькими платежами
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
                and  crs.cso_connects.CHANNEL = 'Активный канал'
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
            
where -- MIN_PERIOD_PAY - CONS_PERIOD_PAY_GRP = 0 --условие выбора только первых последовательных платежей
/*      CONS_PERIOD_PAY_GRP имеет одинаковое значение для CLIE_ID и периодов платежей
        которые идут последовательно друг за другом без пропусков
        к примеру для последовательных периодов платежей 202001, 202002, 202003 
        значение CONS_PERIOD_PAY_GRP будет равно 202001. Специально подобрано (добавлена 1), чтобы 
        первое значение совпадало с первым (минимальным) периодом платежа, что является условием для выбора
        таких платежей. Другие возможные группы последовательных платежей отбрасываются, т.к. 
        для анализа нужны только первым n последовательных платежей.
        К примеру, если после группы последовательных платежей 202001, 200202, 202003
        будет идти последовательность 202005, 202007, 202008, 202009 (в периоды 202004 и 202006 не было платежей), 
        то для 202005 CONS_PERIOD_PAY_GRP будет равно 202002 (уже не равно первому периоду), а для последовательности 
        202007, 202008, 202009 значение будет 202003 (202007 идет 6 по порядку в списке платежей, тогда 202007-6+1=202003)
        Принципиально важно, что CONS_PERIOD_PAY_GRP не являет по сути периодом, а лишь вычисляемым значением,
        который обозначает какую-то группу платежей, сравнение с первым периодом платежа является лишь критерием
        выбора первой группы последовательных платежей       
*/    
        PERIOD_CONNECT - MIN_PERIOD_PAY < 4 -- условие выбора любых платеже за 3 месяца с момента подключения
/*      это условие ограничивает все платежи только теми, которые быыли сделаны в период 3 месяцев.
        При этом если клиент подключен 202001, а платил 202001, 202003, 202004, то отберутся платежи
        202001 (P1*) и 202003 (P2*). Здесь 202003 отмечен как P2 по количеству платежей, а не по календарной последовательности
        периодов 202001, 202002, 202003.
        Здесь есть риск некорректного выбора периодов, связанный с некорректным определением PERIOD_CONNECT.
        Если PERIOD_CONNECT определяется по первой дате подключения клиента, то для последующих подключений
        будут считаться платежи с момента первого подключения. К примеру, если клиент впервые подключен 202001, а после
        были еще подключения 202004 и 202010, то для всех подключений будут показаны платежи за 202001, 202002 и 202003
        Решением данной проблемы может быть расчет Pivot table для каждого периода подключения этого клиента, т.е.
        перед расчетом сводной таблицы определить все подключения клиента и разбить все платежи по этим периодоам подключения,
        учитывая, что для каждого подключения берем не более 3 месяцев с момента подключения и если между подключениями
        прошло меньше 3 периодов (чего по бизнес-логике быть не должно), то берем платежи до следующего подключения
*/

order by CLIE_ID desc, PERIOD_CONNECT    
--    FETCH NEXT 100 ROWS ONLY    
 --   group by PERIOD_CONNECT, CLIE_ID, PERIOD_PAY, AMOUNT_PAY 
