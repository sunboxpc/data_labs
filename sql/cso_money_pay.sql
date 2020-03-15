select *
from crs.cso_money_pay
where PERIOD_CONNECT between 201912 and 202002 and 
	DATE_PAY between TO_TIMESTAMP('01-12-2019 00:00:00') and TO_TIMESTAMP('01-03-2020 00:00:00')
