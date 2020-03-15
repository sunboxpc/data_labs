SELECT agent."FIOPRO", calendar.period_date AS date_sale
FROM (	
		SELECT "FIOPRO", date_trunc('month', MIN("DDATE_DATE_sale")) AS period_date
		FROM scource
		GROUP BY "FIOPRO", "FILIAL", "CHANNEL") AS agent
JOIN (
		SELECT calendar.date:: date as period_date
 		FROM generate_series(
           (SELECT date_trunc('month', MIN("DDATE_DATE_sale")) FROM scource)::timestamp,
           (SELECT MAX("DDATE_DATE_sale") FROM scource)::timestamp,
           interval '1 month'
         	) AS calendar
 		) AS calendar 
		ON agent.period_date >= calendar.period_date
ORDER BY agent."FIOPRO", date_sale
