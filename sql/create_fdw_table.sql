/*
create foreign table fdw_newsales
 (			clie_id numeric,
			subs_id numeric,
			account varchar,
			nz numeric,
			channel varchar,
			channel_detail varchar,
			filial varchar,
			ltc varchar,
			tstet varchar,
			top147 varchar,
			np varchar,
			street varchar,
			house varchar,
			korp varchar,
			apartment varchar,
			phone_room varchar,
			serv_name varchar,
			tp varchar,
			town_id numeric,
			fio varchar,
			fiopro varchar,
  			fiopro_cleared varchar,
			fio_sup varchar,
  			fio_sup_cleared varchar,
			date_sale date,
			brnc_id numeric,
			rtpl_id numeric,
			period numeric,
  			duplicated integer,
  			md5 uuid
 )
SERVER postgres_fdw_server
OPTIONS (table_name 'newsales')
*/
/*
create foreign table fdw_churn
 (			clie_id numeric,
			subs_id numeric,
			nz numeric,
			serv_name varchar,
			name_grp varchar,
			date_churn date
 )
SERVER postgres_fdw_server
OPTIONS (table_name 'churn')
*/
/*
create foreign table fdw_payments
 (			period_connect numeric,
			clie_id numeric,
			date_pay date,
			sum_pay numeric
 )
SERVER postgres_fdw_server
OPTIONS (table_name 'payments')
*/
/*
create foreign table fdw_accruals_eq
 (			period_connect numeric,
			clie_id numeric,
			period_eq numeric,
			sum_eq numeric
)
SERVER postgres_fdw_server
OPTIONS (table_name 'accruals_eq')
*/
/*
create foreign table fdw_accruals_serv
 (			period_connect numeric,
			clie_id numeric,
			subs_id numeric,
			period_serv numeric,
			summ_serv numeric)
SERVER postgres_fdw_server
OPTIONS (table_name 'accruals_serv')			
*/			 