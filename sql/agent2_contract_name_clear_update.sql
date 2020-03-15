--alter table agent2 add column "contract_name_cleared" text

update agent2
set contract_name_cleared = case when (regexp_match(contract_name, '\d{1,4}/\d{2}/\d{1,4}[-/]\d{1,4}'))[1] is null
			then contract_name
			else (regexp_match(contract_name, '\d{1,4}/\d{2}/\d{1,4}[-/]\d{1,4}'))[1]
			end