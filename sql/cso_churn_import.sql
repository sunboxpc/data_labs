 insert into churn (clie_id, subs_id, name_grp, date_churn)
 select clie_id, subs_id, name_grp, date_churn
 from CSO_DISCONNECTS
 on conflict do nothing