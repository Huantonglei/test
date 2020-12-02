select count(recheck_task_id)
from realtime_dwd.dwd_recheck_task as a
where  task_created_time between '2020-09-25 00:00:00' and '2020-09-25 23:59:59'
	and recheck_status in (5,6)
	and recheck_result<>9
	and source_type in (3,4,5,6,13,14,19)
	and recheck_before_audit = 1
	and order_type = 1 and collect_clue_type = 0
1096

select count(recheck_task_id)
from guazi_dw_dwd.dwd_com_recheck_task_ymd as a
where   dt = '${date_y_m_d}'
    and created_at between @startTime and @endTime
	and recheck_status in (5,6)
	and recheck_result<>9
	and source_type in (3,4,5,6,13,14,19)
	and recheck_before_audit = 1
	and order_type = 1 and collect_clue_type = 0
1059