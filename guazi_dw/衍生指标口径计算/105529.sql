select count(*) from realtime_dwd.dwd_consignment_snapshot as a
where  is_consigned = 1
	and discard_time between '2020-10-24 00:00:00' and '2020-10-24 23:59:59'
	and collect_clue_type = 0
12
select count(*)
from guazi_dw_dwd.dwd_consign_collect_contract_snapshot_ymd as a
where dt = '${date_y_m_d}'
	and is_consigned = 1
	and discard_time between @startTime and @endTime
	and collect_clue_type = 0
12

离线表无退车时间：discard_time