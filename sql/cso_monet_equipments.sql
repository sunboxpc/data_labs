select * 
from crs.cso_money_equipments 
--where ROWNUM =1
where PERIOD_CONNECT between 201912 and 202002 and (PERIOD_EQ - PERIOD_CONNECT) between 0 and 4;
