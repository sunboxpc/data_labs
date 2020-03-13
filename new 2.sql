select process_number as номер_наряда, cast('0' as text) as внешний_номер, assignment_type as тип_наряда, date_end as дата_закрытия, order_status as статус_наряда, 
	assignee as исполнитель, personnel_number as ид_исполнителя, contract_type as тип_договора, service_type as тип_услуги, technologies as технология, 
	cast('0' as text) as тип_работ, cast('0' as text) as тип_задания, work as работы, code_description as название_задания, building_type as тип_застройки, 0.0 as баллы_автомат, 
	0.0 as баллы_вручную, mark_value as баллы_итого, mark_by_serv as баллы_базовые, mark_by_add as баллы_доп, cast('0' as text) as статус_исполнения_в_срок, 
	client_type as сегмент_клиента, cast('0' as text) as тип_клиента, mrf_id as мрф_ид, mrf_name as мрф, filial_id as рф_ид, filial as рф, locality as город, 
	cast('0' as text) as организация_линии, ota_count as ota, spd_count as spd, iptv_count as iptv, ktv_count as ktv, cast(0 as numeric) as ctv, code_name as code_name, 
	flag_close_inst as flag_close_inst, load_dttm as load_dttm, src_id as src_id, package_id as package_id, deleted_ind as deleted_ind, 
	wf_run_id as wf_run_id, upd_wf_run_id as upd_wf_run_id, eff_dttm as eff_dttm, exp_dttm as exp_dttm, upload_dttm as upload_dttm
from edw_ods.t_000101_wfm_installation -- Урал
where date_end between to_timestamp('2020-01-01', 'YYYY-MM-DD') and to_timestamp('2020-02-29', 'YYYY-MM-DD')
and order_status = 'Закрыт'

union all

select process_number as номер_наряда, cast('0' as text) as внешний_номер, assignment_type as тип_наряда, date_end as дата_закрытия, order_status as статус_наряда, 
	assignee as исполнитель, personnel_number as ид_исполнителя, contract_type as тип_договора, service_type as тип_услуги, technologies as технология, 
	cast('0' as text) as тип_работ, cast('0' as text) as тип_задания, work as работы, code_description as название_задания, building_type as тип_застройки, 0.0 as баллы_автомат, 
	0.0 as баллы_вручную, mark_value as баллы_итого, mark_by_serv as баллы_базовые, mark_by_add as баллы_доп, cast('0' as text) as статус_исполнения_в_срок, 
	client_type as сегмент_клиента, cast('0' as text) as тип_клиента, mrf_id as мрф_ид, mrf_name as мрф, filial_id as рф_ид, filial as рф, locality as город, 
	cast('0' as text) as организация_линии, ota_count as ota, spd_count as spd, iptv_count as iptv, ktv_count as ktv, cast(0 as numeric) as ctv, code_name as code_name, 
	flag_close_inst as flag_close_inst, load_dttm as load_dttm, src_id as src_id, package_id as package_id, deleted_ind as deleted_ind, 
	wf_run_id as wf_run_id, upd_wf_run_id as upd_wf_run_id, eff_dttm as eff_dttm, exp_dttm as exp_dttm, upload_dttm as upload_dttm
from edw_ods.t_000094_wfm_installation -- Северо-Запад
where date_end between to_timestamp('2020-01-01', 'YYYY-MM-DD') and to_timestamp('2020-02-29', 'YYYY-MM-DD')
and order_status = 'Закрыт'

union all

select process_number as номер_наряда, cast('0' as text) as внешний_номер, assignment_type as тип_наряда, date_end as дата_закрытия, order_status as статус_наряда, 
	assignee as исполнитель, personnel_number as ид_исполнителя, contract_type as тип_договора, service_type as тип_услуги, technologies as технология, 
	cast('0' as text) as тип_работ, cast('0' as text) as тип_задания, work as работы, code_description as название_задания, building_type as тип_застройки, 0.0 as баллы_автомат, 
	0.0 as баллы_вручную, mark_value as баллы_итого, mark_by_serv as баллы_базовые, mark_by_add as баллы_доп, cast('0' as text) as статус_исполнения_в_срок, 
	client_type as сегмент_клиента, cast('0' as text) as тип_клиента, mrf_id as мрф_ид, mrf_name as мрф, filial_id as рф_ид, filial as рф, locality as город, 
	cast('0' as text) as организация_линии, ota_count as ota, spd_count as spd, iptv_count as iptv, ktv_count as ktv, cast(0 as numeric) as ctv, code_name as code_name, 
	flag_close_inst as flag_close_inst, load_dttm as load_dttm, src_id as src_id, package_id as package_id, deleted_ind as deleted_ind, 
	wf_run_id as wf_run_id, upd_wf_run_id as upd_wf_run_id, eff_dttm as eff_dttm, exp_dttm as exp_dttm, upload_dttm as upload_dttm
from edw_ods.t_000025_wfm_installation -- Волга
where date_end between to_timestamp('2020-01-01', 'YYYY-MM-DD') and to_timestamp('2020-02-29', 'YYYY-MM-DD')
and order_status = 'Закрыт'

union all

select requestnumid as номер_наряда, extnumbers as внешний_номер, cast('0' as varchar(255)) as тип_наряда, requestscheduledb as дата_закрытия, requeststatusname as статус_наряда, 
	executors as исполнитель, cast('0' as varchar(1024)) as ид_исполнителя, resourcetype as тип_договора, cast('0' as varchar(240)) as тип_услуги, technologyname as технология, actionname as тип_работ, 
	task_group as тип_задания, cast('0' as varchar(4000)) as работы, tasktypename as название_задания, buildingtype as тип_застройки, autorate as баллы_автомат, 
	manualrate as баллы_вручную, sumrate as баллы_итого, base_points as баллы_базовые, additional_points as баллы_доп, deadline as статус_исполнения_в_срок, 
	customer_type as сегмент_клиента, client_type as тип_клиента, mrf_id as мрф_ид, mrf as мрф, rf_id as рф_ид, rf as рф, city as город, 
	org_line as организация_линии, phone as ota, internet as spd, iptv as iptv, 0 as ktv, ctv as ctv, cast('0' as varchar(32)) as code_name, cast(0 as numeric) as flag_close_inst, 
	load_dttm as load_dttm, src_id as src_id, package_id as package_id, 0 as deleted_ind, 0 as wf_run_id, 0 as upd_wf_run_id, to_timestamp('1900-01-01', 'YYYY-MM-DD') as eff_dttm, to_timestamp('1900-01-01', 'YYYY-MM-DD') as exp_dttm, 
	to_timestamp('1900-01-01', 'YYYY-MM-DD') as upload_dttm
from edw_ods.t_000117_report3wfm -- Дальний Восток
where requestscheduledb between to_timestamp('2020-01-01', 'YYYY-MM-DD') and to_timestamp('2020-02-29', 'YYYY-MM-DD')
and requeststatusname = 'Выполнена' --and task_group = 'Инсталляция' 

union all

select requestnumid as номер_наряда, extnumbers as внешний_номер, cast('0' as varchar(255)) as тип_наряда, requestscheduledb as дата_закрытия, requeststatusname as статус_наряда, 
	executors as исполнитель, cast('0' as varchar(1024)) as ид_исполнителя, resourcetype as тип_договора, cast('0' as varchar(240)) as тип_услуги, technologyname as технология, actionname as тип_работ, 
	task_group as тип_задания, cast('0' as varchar(4000)) as работы, tasktypename as название_задания, buildingtype as тип_застройки, autorate as баллы_автомат, 
	manualrate as баллы_вручную, sumrate as баллы_итого, base_points as баллы_базовые, additional_points as баллы_доп, deadline as статус_исполнения_в_срок, 
	customer_type as сегмент_клиента, client_type as тип_клиента, mrf_id as мрф_ид, mrf as мрф, rf_id as рф_ид, rf as рф, city as город, 
	org_line as организация_линии, phone as ota, internet as spd, iptv as iptv, 0 as ktv, ctv as ctv, cast('0' as varchar(32)) as code_name, cast(0 as numeric) as flag_close_inst, 
	load_dttm as load_dttm, src_id as src_id, package_id as package_id, 0 as deleted_ind, 0 as wf_run_id, 0 as upd_wf_run_id, to_timestamp('1900-01-01', 'YYYY-MM-DD') as eff_dttm, to_timestamp('1900-01-01', 'YYYY-MM-DD') as exp_dttm, 
	to_timestamp('1900-01-01', 'YYYY-MM-DD') as upload_dttm
from edw_ods.t_000093_report3wfm -- Сибирь
where requestscheduledb between to_timestamp('2020-01-01', 'YYYY-MM-DD') and to_timestamp('2020-02-29', 'YYYY-MM-DD')
and requeststatusname = 'Выполнена' --and task_group = 'Инсталляция' 

union all

select requestnumid as номер_наряда, extnumbers as внешний_номер, cast('0' as varchar(255)) as тип_наряда, requestscheduledb as дата_закрытия, requeststatusname as статус_наряда, 
	executors as исполнитель, cast('0' as varchar(1024)) as ид_исполнителя, resourcetype as тип_договора, cast('0' as varchar(240)) as тип_услуги, technologyname as технология, actionname as тип_работ, 
	task_group as тип_задания, cast('0' as varchar(4000)) as работы, tasktypename as название_задания, buildingtype as тип_застройки, autorate as баллы_автомат, 
	manualrate as баллы_вручную, sumrate as баллы_итого, base_points as баллы_базовые, additional_points as баллы_доп, deadline as статус_исполнения_в_срок, 
	customer_type as сегмент_клиента, client_type as тип_клиента, mrf_id as мрф_ид, mrf as мрф, rf_id as рф_ид, rf as рф, city as город, 
	org_line as организация_линии, phone as ota, internet as spd, iptv as iptv, 0 as ktv, ctv as ctv, cast('0' as varchar(32)) as code_name, cast(0 as numeric) as flag_close_inst, 
	load_dttm as load_dttm, src_id as src_id, package_id as package_id, 0 as deleted_ind, 0 as wf_run_id, 0 as upd_wf_run_id, to_timestamp('1900-01-01', 'YYYY-MM-DD') as eff_dttm, to_timestamp('1900-01-01', 'YYYY-MM-DD') as exp_dttm, 
	to_timestamp('1900-01-01', 'YYYY-MM-DD') as upload_dttm
from edw_ods.t_000079_report3wfm -- Центр
where requestscheduledb between to_timestamp('2020-01-01', 'YYYY-MM-DD') and to_timestamp('2020-02-29', 'YYYY-MM-DD')
and requeststatusname = 'Выполнена'-- and task_group = 'Инсталляция' 
