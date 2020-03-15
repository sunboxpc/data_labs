/*create SERVER oradb FOREIGN DATA WRAPPER oracle_fdw
          OPTIONS (dbserver '(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=ECODWH.UR.RT.RU)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=ecodwh.ur.rt.ru)))')
         
GRANT USAGE ON FOREIGN SERVER oradb TO postgres


CREATE USER MAPPING FOR postgres SERVER oradb
          OPTIONS (user 'осо_glukhov_sv', password '****')
 


drop foreign table cso_connects

CREATE FOREIGN table CSO_CONNECTS (
				CLIE_ID numeric,
				SUBS_ID numeric,
				ACCOUNT varchar,
				NZ numeric,
				CHANNEL varchar,
				CHANNEL_DETAIL varchar,
				FILIAL varchar,
				LTC varchar,
				TSTET varchar,
				TOP147 varchar,
				NP varchar,
				STREET varchar,
				HOUSE varchar,
				KORP varchar,
				APARTMENT varchar,
				PHONE_ROOM varchar,
				SERV_NAME varchar,
				TP varchar,
				TOWN_ID numeric,
				FIO varchar,
				FIOPRO varchar,
				FIO_SUP varchar,
				DATE_SALE date,
				BRNC_ID numeric,
				RTPL_ID numeric,
				PERIOD numeric       

       ) SERVER oradb OPTIONS (schema 'CRS', table 'CSO_CONNECTS')
*/
CREATE FOREIGN table CSO_DISCONNECTS (
				CLIE_ID numeric,
				SUBS_ID numeric,
				NZ numeric,
				SERV_NAME varchar,
				NAME_GRP varchar,
				DATE_CHURN date,
				PERIOD numeric
           ) SERVER oradb OPTIONS (schema 'CRS', table 'CSO_DISCONNECTS')

CREATE FOREIGN table CSO_MONEY_BASIC_SERVICES
 (			PERIOD_CONNECT numeric,
			CLIE_ID numeric,
			SUBS_ID numeric,
			PERIOD_SERV numeric,
			SUMM_SERV numeric
           ) SERVER oradb OPTIONS (schema 'CRS', table 'CSO_MONEY_BASIC_SERVICES')
           
 CREATE FOREIGN table CSO_MONEY_EQUIPMENTS
 (			PERIOD_CONNECT numeric,
			CLIE_ID numeric,
			PERIOD_EQ numeric,
			SUM_EQ numeric
           ) SERVER oradb OPTIONS (schema 'CRS', table 'CSO_MONEY_EQUIPMENTS')
           
   CREATE FOREIGN table CSO_MONEY_PAY
 (			PERIOD_CONNECT numeric,
			CLIE_ID numeric,
			DATE_PAY date,
			SUM_PAY numeric
           ) SERVER oradb OPTIONS (schema 'CRS', table 'CSO_MONEY_PAY')
