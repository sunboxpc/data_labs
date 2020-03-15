CREATE FOREIGN TABLE ft_accruals_b2b (
        cfo bigint  NULL,
    account bigint  NULL,
    inn bigint  NULL,
    client_name text  NULL,
    contract_num text  NULL,
    contract_detail text  NULL,
    segent_type text  NULL,
    serv_code text  NULL,
    r_code text  NULL,
    accrual_sum_vo_vat double precision  NULL,
    negative_correction_sum_vo_vat double precision  NULL,
    positive_correction_sum_vo_vat double precision  NULL,
    correcting_period double precision  NULL,
    serv_number double precision  NULL,
    comment text  NULL,
   related_accounts text  NULL,
    file text  NULL,
    accrual_sum_vo_vat2 double precision  NULL,
    id integer

)
        SERVER postgres_fdw_server
        OPTIONS (schema_name 'public', table_name 'accruals_b2b');