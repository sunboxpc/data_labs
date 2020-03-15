--alter table newsales add column agent_id uuid 
update newsales
set agent_id = (md5(CAST(filial || fiopro_cleared || channel as text)))::uuid