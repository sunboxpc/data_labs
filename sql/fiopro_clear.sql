/*alter table newsales 
add column fiopro_cleared text not null default 'n/a',
add column fio_sup_cleared text not null default 'n/a'
*/
--alter table agent_pay_calc add column fiopro_cleared text
update agent_pay_calc
set fiopro_cleared = case 
						when seller is null then 'n/a'
						else 
		trim(upper(regexp_replace(
		regexp_replace(
		regexp_replace(
		regexp_replace(
		regexp_replace(
		regexp_replace(
		regexp_replace(
		regexp_replace(
		regexp_replace(
		regexp_replace(
		regexp_replace(
		regexp_replace(
		regexp_replace(
		regexp_replace(
		regexp_replace(
		regexp_replace(seller, 
	   	'(^[а-яА-Яa-zA-Z]{1,4}[\-_]+)|(\s[a-zA-Zа-яА-Я]{1,4}[\-_]+)', ''),
	   	'[\s]{2,}',''),
	   	'[eEЁ]','Е'),
	   	'A','А'),
	  	'B','В'),
		'C','С'),
		'H','Н'),
	  	'K','К'),
	  	'M','М'),
	  	'[0O]','О'),
	  	'P','Р'),
	  	'T','Т'),
	  	'X','Х'),
	  	'Y','У'),
  		'N','П'),
	  	'3','З')))
		end
