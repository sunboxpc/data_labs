/*delete 
from install
where баллы like 'Баллы'
--where num=0.0


select distinct баллы, replace(баллы, ',','.')::double precision
from install
where replace(баллы, ',','.')::double precision <= 0.0


select distinct дата_исполнения_факт, to_timestamp(дата_исполнения_факт, 'DD.MM.YYYY HH24:MI:SS')
from install
limit 100
*/
update install
set баллы = replace(баллы, ',','.')::double precision,
дата_исполнения_факт = to_timestamp(дата_исполнения_факт, 'DD.MM.YYYY HH24:MI:SS')
