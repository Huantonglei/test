select count(clue_id)
from realtime_dwd.dwd_consignment_snapshot as a
where  discard_time < audit_time
	and substr(audit_time,1,10) = substr(task_create_time,1,10)
	and is_consigned in (1,3)
	and audit_time between '2020-10-24 00:00:00' and '2020-10-24 23:59:59'
	and collect_clue_type = 0
147

select count(clue_id)
from guazi_dw_dwd.dwd_consign_collect_contract_snapshot_ymd as a
where  dt = '${date_y_m_d}'
    and audit_time between @startTime and @endTime
	and (discard_time < audit_time or discard_time is null)
	and substr(audit_time,1,10) = substr(task_create_time,1,10)
	and is_consigned in (1,3)
 	and collect_clue_type = 0
152
 	0
 ！！！离线表中 discard_time为空，所以查询为 0

 用  (discard_time < audit_time or discard_time is null)