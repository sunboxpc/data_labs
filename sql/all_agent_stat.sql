--with res_tbl as (
select total_pay.code, total_pay.agent_code, total_pay.fiopro as fio, 
			total_pay.vedomost, 
			total_pay.accrual_date, agent_profile.contract_details, agent_profile.contract_type,
			esed.reg_num_cleared, esed.reg_num, esed.reg_data,
			esed.amount_with_vat,esed.amount_wo_vat,esed.na_document,esed.doc_type,
			agent_profile.department, agent_profile.filial_code, agent_profile.filial_name,
			total_pay.paid,	serv_pay.spd_agent_pay_fact, serv_pay.iptv_agent_pay_fact,	
			serv_pay.ota_agent_pay_fact, serv_pay.mvno_agent_pay_fact, serv_pay.ktv_agent_pay_fact,
			serv_pay.end_point_eq, serv_pay.smart_house_agent_pay_fact,
			serv_pay.cctv_smart_house_agent_pay_fact, serv_pay.other_eq_agent_pay_fact,
			serv_pay.web_ks_agent_pay_fact, serv_pay.cctv_b2b_agent_pay_fact,
			(case when total_pay.agent_code in (
				select split_part(code,'-',1) as agent_code
				from agent
				group by split_part(code,'-',1)
				having sum("sum") > 500000)
			then true else false end) as bigger500kRUB
	from (
		select 	code, UPPER(TRIM(fio)) 	as fiopro, 
				split_part(code,'-',1) as agent_code,
				vedomost, 
				date_trunc('month', To_Timestamp_From_Excel(prepaid_data:: integer))- interval '1 month'
				as accrual_date,
				sum("sum") as paid
		from agent
		group by accrual_date, fio, vedomost, code
		order by vedomost, fiopro, accrual_date
		) as total_pay
	left join (
		select 	code, contract_name_cleared,
				string_agg(distinct code || ' ' || contract_name || ' : ' || contract_start_data, 
						   chr(10)) contract_details,
				string_agg(distinct contract_type, 
						   chr(10)) contract_type,
				string_agg(distinct department, 
						   chr(10)) department,
				string_agg(distinct left(department_sys_name,5), 
						   chr(10))  filial_code,
				string_agg(distinct split_part(department_full_name, $$\$$,1), 
						   chr(10)) filial_name
		from agent2
		group by code, contract_name_cleared
		) as agent_profile using (code)
	full join (
		select code, fiopro, accrual_date, sum("3004070000") spd_agent_pay_fact,  
			sum("3004050000") iptv_agent_pay_fact,	sum("3004030000") ota_agent_pay_fact, 
			sum("3004080000") mvno_agent_pay_fact, sum("3004060000") ktv_agent_pay_fact,
			sum("3004590000") end_point_eq, sum("3004150500") smart_house_agent_pay_fact,
			sum("3004150400") cctv_smart_house_agent_pay_fact, sum("3004600000") other_eq_agent_pay_fact,
			sum("3004090000") web_ks_agent_pay_fact, sum("3004150100") cctv_b2b_agent_pay_fact 
		from crosstab(
			$$ select dense_rank() over (order by code, fiopro, account, accrual_date)::int row_name,
				code, fiopro,  accrual_date, account, ceiling(sum(paid)) paid
			from (
				select code, UPPER(TRIM(fio)) fiopro, account, dt_sum paid,
						date_trunc('month', To_Timestamp_From_Excel(period:: integer))- interval '1 month'
						as accrual_date
				from agent_pay
				where account like '3%' 
		--			and code in (select code from agent where vedomost like '015%' )
		/*					   and code in (
								   select distinct code 
								   from agent2 where contract_type like '%ГПХ%')*/
				) as srs
			group by code, fiopro, account, accrual_date
			order by fiopro, accrual_date, account $$,
			$$ values	('3004030000'::text), ('3004050000'::text), ('3004060000'::text), 
						('3004070000'::text), ('3004080000'::text), ('3004090000'::text), 
						('3004150100'::text),('3004150400'::text), ('3004150500'::text),
						('3004590000'::text), ('3004600000'::text) $$) tt
			--$$ select distinct account from agent_pay 
			--	where code in (select code from agent where vedomost like '015%')
			--	and account like '3%'order by 1$$) tt
			(row_name int, code text, fiopro text, accrual_date timestamp,
						"3004030000" int, "3004050000" int, "3004060000" int, 
						"3004070000" int, "3004080000" int, "3004090000" int, 
						"3004150100" int, "3004150400" int, "3004150500" int, 
						"3004590000" int, "3004600000" int)
			group by code, fiopro, accrual_date
		) as serv_pay using(code, accrual_date)
	left join (
		select 	reg_num_cleared, 
				string_agg(distinct reg_num, chr(10)) as reg_num, 
				string_agg(distinct reg_data, chr(10)) as reg_data,
				max(amount_with_vat) as amount_with_vat,
				max(amount_wo_vat) as amount_wo_vat, 
				string_agg(distinct na_document, chr(10)) as na_document,
				string_agg(distinct doc_type, chr(10)) as doc_type
		from esed_all
		group by reg_num_cleared
		) as esed on agent_profile.contract_name_cleared = esed.reg_num_cleared
--where total_pay.fiopro like upper('Бежуткин Сергей Владимирович') and accrual_date between '2019-02-01' and '2019-03-01'