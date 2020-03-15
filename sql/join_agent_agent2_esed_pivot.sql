select distinct t.code, t.reg_num_cleared, t.reg_num, t.reg_data,t.doc_type, 
				t.amount_with_vat, t.amount_wo_vat, 
				t.prepaid_id,  t."sum", t.prepaid_date, 
				t.na_document, t.empty_sum, t.zero_sum
from (	select 	t1.code, t3.reg_num_cleared, t3.reg_num, t3.reg_data, 
	  			t3.amount_with_vat, t3.amount_wo_vat, 
				t1.prepaid_id,  t1.sum, 
	  			to_timestamp_from_excel(CAST(t1.prepaid_data as integer)) as prepaid_date,
				t3.na_document, t3.empty_sum, t3.zero_sum, t3.doc_type
		from agent as t1
		full join agent2 as t2 on t1.code = t2.code
		full join esed_all as t3 on t2.contract_name_cleared = t3.reg_num_cleared
	  	where t1.code is not null and t1.code = '425036-3' 
	  		and (contract_group is null or contract_group in (
		'ВГР. Агентский договор (заключение договоров)',
	 	'ВГР. Договор "содействия" (агентские/возмездные услуги)',
	 	'ВГР. Прочие договоры',
	 	'ДВЗ.Прочие договоры',
	 	'Дог. на оказание консультационных услуг',
		'Договор на оказание услуг по обучению',
		'Договор на оказание услуг технического обслуживания',
	 	'Договор на приобретение программного обеспечения',
		'Договора гражданско-правового характера',
	 	'Дополнительное соглашение',
	 	'НИ.Агентский договор',
		'НИ.Договор возмездного оказания услуг',
	 	'Прочие агентские договоры',
		'Прочие договоры',
	 	'Прочие договоры возмездного оказания услуг',
		'Прочие договоры подряда',
		'агентирование',
		'гражданско-правового характера',
		'подряд'
		))
	) as t
order by t.code, t.prepaid_date asc, t.reg_num_cleared desc
