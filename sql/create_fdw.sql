--CREATE EXTENSION postgres_fdw
/*CREATE SERVER postgres_fdw_server FOREIGN DATA WRAPPER postgres_fdw
OPTIONS (host '10.35.19.30', dbname 'clie_db')

CREATE USER MAPPING FOR PUBLIC SERVER postgres_fdw_server
OPTIONS (password '')


CREATE FOREIGN TABLE fdw_newsales (greeting TEXT)
SERVER postgres_fdw_server
OPTIONS (table_name 'newsales')

*/