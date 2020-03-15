--alter table esed_all add column "empty_sum" bool
--alter table esed_all add column "zero_sum" bool
--alter table esed_all add column "reg_num_cleared" text

update esed_all
set na_document = quote_literal(na_document)
/*
set reg_num_cleared = case when (regexp_match(reg_num, '\d{1,4}/\d{2}/\d{1,4}[-/]\d{1,4}'))[1] is null
			then reg_num
			else (regexp_match(reg_num, '\d{1,4}/\d{2}/\d{1,4}[-/]\d{1,4}'))[1]
			end
set zero_sum = case when amount_with_vat > 0 or amount_wo_vat > 0
				then false
				else true
				end
set empty_sum = case when amount_with_vat is null and amount_wo_vat is null
				then true
				else false
				end


select quote_literal(na_document)
from esed_all
limit 10*/