select process_number as номер_наряда, 0 as внешний_номер, assignment_type as тип_наряда, date_end as дата_закрытия, 
	order_status as статус_наряда, assignee as исполнитель, personnel_number as ид_исполнителя, contract_type as тип_договора, 
	service_type as тип_услуги, technologies as технология, 0 as тип_работ, 0 as тип_задания, work as работы, 
	code_description as название_задания, building_type as тип_застройки, 0 as баллы_автомат, 0 as баллы_вручную, 
	mark_value as баллы_итого, mark_by_serv as баллы_базовые, mark_by_add as баллы_доп, 0 as статус_исполнения_в_срок, 
	client_type as сегмент_клиента, 0 as тип_клиента, mrf_id as мрф_ид, mrf_name as мрф, filial_id as рф_ид, 
	filial as рф, locality as город, 0 as организация_линии, ota_count as ota, spd_count as spd, iptv_count as iptv, 
	ktv_count as ktv, 0 as ctv, code_name as code_name, flag_close_inst as flag_close_inst, load_dttm as load_dttm, 
	src_id as src_id, package_id as package_id, deleted_ind as deleted_ind, wf_run_id as wf_run_id, upd_wf_run_id as upd_wf_run_id, 
	eff_dttm as eff_dttm, exp_dttm as exp_dttm, upload_dttm as upload_dttm
from edw_ods.t_000101_wfm_installation -- Урал
where date_end between to_timestamp('2020-01-01', 'YYYY-MM-DD') and to_timestamp('2020-02-29', 'YYYY-MM-DD')
and order_status = 'Закрыт'

union all

select process_number as номер_наряда, 0 as внешний_номер, assignment_type as тип_наряда, date_end as дата_закрытия, 
	order_status as статус_наряда, assignee as исполнитель, personnel_number as ид_исполнителя, contract_type as тип_договора, 
	service_type as тип_услуги, technologies as технология, 0 as тип_работ, 0 as тип_задания, work as работы, 
	code_description as название_задания, building_type as тип_застройки, 0 as баллы_автомат, 0 as баллы_вручную, 
	mark_value as баллы_итого, mark_by_serv as баллы_базовые, mark_by_add as баллы_доп, 0 as статус_исполнения_в_срок, 
	client_type as сегмент_клиента, 0 as тип_клиента, mrf_id as мрф_ид, mrf_name as мрф, filial_id as рф_ид, 
	filial as рф, locality as город, 0 as организация_линии, ota_count as ota, spd_count as spd, iptv_count as iptv, 
	ktv_count as ktv, 0 as ctv, code_name as code_name, flag_close_inst as flag_close_inst, load_dttm as load_dttm, 
	src_id as src_id, package_id as package_id, deleted_ind as deleted_ind, wf_run_id as wf_run_id, upd_wf_run_id as upd_wf_run_id, 
	eff_dttm as eff_dttm, exp_dttm as exp_dttm, upload_dttm as upload_dttm
from edw_ods.t_000094_wfm_installation -- Северо-Запад
where date_end between to_timestamp('2020-01-01', 'YYYY-MM-DD') and to_timestamp('2020-02-29', 'YYYY-MM-DD')
and order_status = 'Закрыт'

union all

select process_number as номер_наряда, 0 as внешний_номер, assignment_type as тип_наряда, date_end as дата_закрытия, 
	order_status as статус_наряда, assignee as исполнитель, personnel_number as ид_исполнителя, contract_type as тип_договора, 
	service_type as тип_услуги, technologies as технология, 0 as тип_работ, 0 as тип_задания, work as работы, 
	code_description as название_задания, building_type as тип_застройки, 0 as баллы_автомат, 0 as баллы_вручную, 
	mark_value as баллы_итого, mark_by_serv as баллы_базовые, mark_by_add as баллы_доп, 0 as статус_исполнения_в_срок, 
	client_type as сегмент_клиента, 0 as тип_клиента, mrf_id as мрф_ид, mrf_name as мрф, filial_id as рф_ид, 
	filial as рф, locality as город, 0 as организация_линии, ota_count as ota, spd_count as spd, iptv_count as iptv, 
	ktv_count as ktv, 0 as ctv, code_name as code_name, flag_close_inst as flag_close_inst, load_dttm as load_dttm, 
	src_id as src_id, package_id as package_id, deleted_ind as deleted_ind, wf_run_id as wf_run_id, upd_wf_run_id as upd_wf_run_id, 
	eff_dttm as eff_dttm, exp_dttm as exp_dttm, upload_dttm as upload_dttm
from edw_ods.t_000025_wfm_installation -- Волга
where date_end between to_timestamp('2020-01-01', 'YYYY-MM-DD') and to_timestamp('2020-02-29', 'YYYY-MM-DD')
and order_status = 'Закрыт'