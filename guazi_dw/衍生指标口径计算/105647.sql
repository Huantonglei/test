select count(recheck_task_id)
from realtime_dwd.dwd_recheck_task as a
where  recheck_status in (5,6)
    and recheck_result <> 9
    and is_open_platform = 0
    and submit_time between '2020-10-24 00:00:00' and '2020-10-24 23:59:59'
    and collect_clue_type = 0
1836

select count(recheck_task_id)
from guazi_dw_dwd.dwd_com_recheck_task_ymd as a
where  dt = '${date_y_m_d}'
	and submit_time between @startTime and @endTime
    and recheck_status in (5,6)
    and recheck_result <> 9
    and is_open_platform = 0
    and collect_clue_type = 0

1836