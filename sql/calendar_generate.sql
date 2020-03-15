select cast(cast('2019-01-01' as date) + (n || 'month'):: interval as date)
from generate_series(0,12) n