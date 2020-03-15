select distinct split_part(contract_name, ' ',1), contract_name
from agent2
where exists (select * from regexp_matches(contract_name, '^\d{1,4}/\d{2}/\d{1,4}[-/]\d{1,4}'))
and contract_name <> split_part(contract_name, ' ',1)
order by split_part(contract_name, ' ',1)