--create SERVER oradb FOREIGN DATA WRAPPER oracle_fdw
--          OPTIONS (dbserver '(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=ECODWH.UR.RT.RU)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=ecodwh.ur.rt.ru)))')
         
--GRANT USAGE ON FOREIGN SERVER oradb TO postgres


--CREATE USER MAPPING FOR postgres SERVER oradb
--         OPTIONS (user 'осо_glukhov_sv', password 'rdm71tq')
/* 
CREATE FOREIGN TABLE oratab (
          subs_id   integer  NOT NULL,
          period_serv integer
       ) SERVER oradb OPTIONS (schema 'ECODWH.UR.RT.RU', table 'crs.cso_money_basic_services')
*/
select *
from oratab
	   