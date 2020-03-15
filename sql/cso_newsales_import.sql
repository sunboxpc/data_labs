 insert into newsales (clie_id,	subs_id,	account,	nz,	channel,	channel_detail,	filial,	ltc,	tstet,	top147,	np,	street,	house,	korp,	apartment,	phone_room,	serv_name,	tp,	town_id,	fio,	fiopro,	fio_sup,	date_sale,	brnc_id,	rtpl_id,	period)
 select clie_id,	subs_id,	account,	nz,	channel,	channel_detail,	filial,	ltc,	tstet,	top147,	np,	street,	house,	korp,	apartment,	phone_room,	serv_name,	tp,	town_id,	fio,	fiopro,	fio_sup,	date_sale,	brnc_id,	rtpl_id,	period
 from CSO_CONNECTS
 on conflict do nothing