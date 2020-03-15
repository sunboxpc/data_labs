select requestnumid as номер_наряда, extnumbers as внешний_номер, 0 as тип_наряда, requestscheduledb as дата_закрытия, 
	requeststatusname as статус_наряда, executors as исполнитель, 0 as ид_исполнителя, 
	resourcetype as тип_договора, 0 as тип_услуги, technologyname as технология, actionname as тип_работ, 
	task_group as тип_задания, 0 as работы, tasktypename as название_задания, buildingtype as тип_застройки, 
	autorate as баллы_автомат, manualrate as баллы_вручную, sumrate as баллы_итого, base_points as баллы_базовые, 
	additional_points as баллы_доп, deadline as статус_исполнения_в_срок, customer_type as сегмент_клиента, 
	client_type as тип_клиента, mrf_id as мрф_ид, mrf as мрф, rf_id as рф_ид, rf as рф, city as город, 
	org_line as организация_линии, phone as ota, internet as spd, iptv as iptv, 0 as ktv, ctv as ctv, 0 as code_name, 
	0 as flag_close_inst, load_dttm as load_dttm, src_id as src_id, package_id as package_id, 0 as deleted_ind, 
	0 as wf_run_id, 0 as upd_wf_run_id, 1 as eff_dttm, 1 as exp_dttm, 1 as upload_dttm
from edw_ods.t_000117_report3wfm -- Дальний Восток
where requestscheduledb between to_timestamp('2020-01-01', 'YYYY-MM-DD') and to_timestamp('2020-02-29', 'YYYY-MM-DD')
and requeststatusname = 'Выполнена' --and task_group = 'Инсталляция' 

union all

select requestnumid as номер_наряда, extnumbers as внешний_номер, 0 as тип_наряда, requestscheduledb as дата_закрытия, 
	requeststatusname as статус_наряда, executors as исполнитель, 0 as ид_исполнителя, 
	resourcetype as тип_договора, 0 as тип_услуги, technologyname as технология, actionname as тип_работ, 
	task_group as тип_задания, 0 as работы, tasktypename as название_задания, buildingtype as тип_застройки, 
	autorate as баллы_автомат, manualrate as баллы_вручную, sumrate as баллы_итого, base_points as баллы_базовые, 
	additional_points as баллы_доп, deadline as статус_исполнения_в_срок, customer_type as сегмент_клиента, 
	client_type as тип_клиента, mrf_id as мрф_ид, mrf as мрф, rf_id as рф_ид, rf as рф, city as город, 
	org_line as организация_линии, phone as ota, internet as spd, iptv as iptv, 0 as ktv, ctv as ctv, 0 as code_name, 
	0 as flag_close_inst, load_dttm as load_dttm, src_id as src_id, package_id as package_id, 0 as deleted_ind, 
	0 as wf_run_id, 0 as upd_wf_run_id, 1 as eff_dttm, 1 as exp_dttm, 1 as upload_dttm
from edw_ods.t_000093_report3wfm -- Сибирь
where requestscheduledb between to_timestamp('2020-01-01', 'YYYY-MM-DD') and to_timestamp('2020-02-29', 'YYYY-MM-DD')
and requeststatusname = 'Выполнена' --and task_group = 'Инсталляция' 

union all

select requestnumid as номер_наряда, extnumbers as внешний_номер, 0 as тип_наряда, requestscheduledb as дата_закрытия, 
	requeststatusname as статус_наряда, executors as исполнитель, 0 as ид_исполнителя, 
	resourcetype as тип_договора, 0 as тип_услуги, technologyname as технология, actionname as тип_работ, 
	task_group as тип_задания, 0 as работы, tasktypename as название_задания, buildingtype as тип_застройки, 
	autorate as баллы_автомат, manualrate as баллы_вручную, sumrate as баллы_итого, base_points as баллы_базовые, 
	additional_points as баллы_доп, deadline as статус_исполнения_в_срок, customer_type as сегмент_клиента, 
	client_type as тип_клиента, mrf_id as мрф_ид, mrf as мрф, rf_id as рф_ид, rf as рф, city as город, 
	org_line as организация_линии, phone as ota, internet as spd, iptv as iptv, 0 as ktv, ctv as ctv, 0 as code_name, 
	0 as flag_close_inst, load_dttm as load_dttm, src_id as src_id, package_id as package_id, 0 as deleted_ind, 
	0 as wf_run_id, 0 as upd_wf_run_id, 1 as eff_dttm, 1 as exp_dttm, 1 as upload_dttm
from edw_ods.t_000079_report3wfm -- Центр
where requestscheduledb between to_timestamp('2020-01-01', 'YYYY-MM-DD') and to_timestamp('2020-02-29', 'YYYY-MM-DD')
and requeststatusname = 'Выполнена'-- and task_group = 'Инсталляция' 
 