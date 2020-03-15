 insert into eq_accruals (period_connect, clie_id, period_eq, sum_eq)
 select period_connect, clie_id, period_eq, sum_eq
 from CSO_MONEY_EQUIPMENTS
 on conflict do nothing