INSERT INTO fiopro_db as f (id, filial, channel, fiopro, min_date_sale)
SELECT id, filial, channel, fiopro, min_date_sale FROM temp_table as t
ON CONFLICT (id) DO UPDATE 
    SET min_date_sale = CASE
                            WHEN EXCLUDED.min_date_sale IS NOT NULL AND EXCLUDED.min_date_sale < f.min_date_sale THEN EXCLUDED.min_date_sale
							WHEN f.min_date_sale IS NULL and EXCLUDED.min_date_sale IS NOT NULL THEN EXCLUDED.min_date_sale
							ELSE f.min_date_sale
                        END
    WHERE EXCLUDED.id = f.id
	
RETURNING *