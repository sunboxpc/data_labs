
SELECT DISTINCT t1.fiopro as f1, 
				t2.fiopro as f2, 
				levenshtein(t1.fiopro, t2.fiopro) as diff
FROM (SELECT DISTINCT UPPER(fiopro) as fiopro FROM fiopro_db) as t1,
	 (SELECT DISTINCT UPPER(fiopro) as fiopro FROM fiopro_db) as t2
WHERE t1.fiopro <> t2.fiopro AND levenshtein(t1.fiopro, t2.fiopro) <2 
ORDER BY levenshtein(t1.fiopro, t2.fiopro) DESC
