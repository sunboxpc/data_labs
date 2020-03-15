 insert into serv_accruals (period_connect, clie_id, subs_id, period_serv, summ_serv)
 select period_connect, clie_id, subs_id, period_serv, summ_serv
 from cso_money_basic_services
 on conflict do nothing