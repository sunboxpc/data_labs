INSERT INTO newsales as f (clie_id, subs_id, account, nz, 
	  channel, fio, fiopro_cleared, fio_sup_cleared, 
	  filial, ltc, tstet, top147, np, street, 
	  house, korp, apartment, phone_room, serv_name, 
	  channel_detail, date_sale, fiopro, fio_sup, md5
)
SELECT clie_id, subs_id, account, nz, 
	  channel, fio, fiopro_cleared, fio_sup_cleared, 
	  filial, ltc, tstet, top147, np, street, 
	  house, korp, apartment, phone_room, serv_name, 
	  channel_detail, date_sale, fiopro, fio_sup, md5
FROM temp as t
ON CONFLICT(md5)
DO NOTHING 
--RETURNING *