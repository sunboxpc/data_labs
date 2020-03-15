DELETE FROM newsales
WHERE id IN
    (SELECT id
    FROM 
        (SELECT id,
         ROW_NUMBER() OVER( PARTITION BY 
		clie_id,
		subs_id,
		account,
		nz,
		channel,
		channel_detail,
		filial,
		ltc,
		tstet,
		top147,
		np,
		street,
		house,
		korp,
		apartment,
		phone_room,
		serv_name,
		tp,
		town_id,
		fio,
		fiopro,
		fio_sup,
		date_sale,
		brnc_id,
		rtpl_id,
		period

        ORDER BY  id ) AS row_num
        FROM newsales ) t
        WHERE t.row_num > 1 );