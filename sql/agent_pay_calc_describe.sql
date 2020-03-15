select fiopro_cleared fiopro, superviser fio_sup, month_conection accrual_date, sum(itog_po_stroke) total,
		sum(iptv_itog) total_iptv, sum(internet_itog) total_spd, sum(pstn_itog) total_ota
from agent_pay_calc
group by fiopro_cleared, superviser, month_conection
limit 5