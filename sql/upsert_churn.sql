INSERT INTO churn as f (clie_id, subs_id, nz, serv_name, name_grp, 
	  date_churn, md5
)
SELECT clie_id, subs_id, nz, serv_name, name_grp,
	  date_churn, md5
FROM temp as t
ON CONFLICT(md5)
DO NOTHING 
--RETURNING *