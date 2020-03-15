select 'row_name int, code text, fiopro text, accrual_date timestamp,'
		 || string_agg(DISTINCT quote_ident(account)::text, ' int, ' 
					   ORDER BY quote_ident(account)::text)
		 || ' int'
from (values('3004030000'::text), ('3004050000'::text), ('3004060000'::text), 
	 ('3004070000'::text), ('3004080000'::text), ('3004090000'::text), 
	  ('3004150100'::text),('3004150400'::text), ('3004150500'::text),
	  ('3004590000'::text), ('3004600000'::text)) as v (account)
--from agent_pay
--where code in (select code from agent where vedomost like '015%')
--and account like '3%'