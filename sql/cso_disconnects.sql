select * 
from crs.cso_disconnects 
where exists (	select * 
				from crs.cso_connects 
				where crs.cso_connects.SUBS_ID = crs.cso_disconnects.SUBS_ID 
					and  crs.cso_connects.PERIOD between 201912 and 202002
				) 
			and PERIOD between 201912 and 202002