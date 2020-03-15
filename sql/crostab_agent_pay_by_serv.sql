select fiopro, accrual_date, 
		"3004030000" ota, "3004050000" iptv, "3004060000" ktv, 
	 	"3004070000" spd, "3004080000" mvno, "3004090000" web_ks, 
		"3004150100" cctv_b2b, "3004150400" cctv_smart_house, 
		"3004150500" smart_house, "3004590000" end_point_eq, "3004600000" other_eq
from crosstab(
	$$ select dense_rank() over (order by code, fiopro, account, accrual_date)::int row_name,
		code, fiopro,  accrual_date, account, ceiling(sum(paid)) paid
	from (
		select code, UPPER(TRIM(fio)) fiopro, account, dt_sum paid,
				date_trunc('month', To_Timestamp_From_Excel(period:: integer))- interval '1 month'
				as accrual_date
		from agent_pay
		where code in (select code from agent where vedomost like '015%')
				and account like '3%') ct
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
group by fiopro, accrual_date, "3004030000", "3004050000", "3004060000", 
	 			"3004070000", "3004080000", "3004090000", 
	 			"3004150100", "3004150400", "3004150500", 
	 			"3004590000", "3004600000"