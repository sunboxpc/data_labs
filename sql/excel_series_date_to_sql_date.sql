CREATE OR REPLACE FUNCTION To_Timestamp_From_Excel (ExcelDate integer)
RETURNS timestamp without time zone AS $$
BEGIN
   IF ExcelDate > 59 THEN
    ExcelDate = ExcelDate - 1;
   END IF;
   RETURN date '1899-12-31' + ExcelDate;
END;
$$ LANGUAGE plpgsql;

select distinct date_trunc('month', To_Timestamp_From_Excel(prepaid_data:: integer))  as date,
ceiling (sum("sum")) as paid
from agent
group by date
order by date
