update newsales set duplicated = 1  
where nz in (select nz from (select nz, count(1)
							from newsales
							group by nz
							having count(1)>1
							order by nz) as foo )