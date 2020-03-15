with t1 as (SELECT DISTINCT fiopro  as fiopro, fiopro_cleared as norm, channel FROM newsales),
t2 as (SELECT DISTINCT fiopro  as fiopro, fiopro_cleared as norm FROM newsales),
t3 as (select fiopro, count(subs_id) as sale, period from newsales group by fiopro, period )
SELECT DISTINCT t1.norm as norm1, 
				t2.norm as norm2,
				t1.fiopro as f1, 
				t2.fiopro as f2, 
				levenshtein(t1.norm, t2.norm) as diff,
				t1.channel,
				t3.period,
			t3.sale	
FROM  t1, t2
join t3 t3 on t2.fiopro = t3.fiopro 
WHERE  levenshtein(t1.norm, t2.norm)<2 --and t1.fiopro <> t2.fiopro
ORDER BY t1.norm DESC
