select count(distinct clue_id)
from realtime_dwd.dwd_recheck_task as a
where  recheck_status in (5,6)
    and recheck_result <> 9
    and is_open_platform = 0
    and task_created_time between '2020-09-25 00:00:00'
    and '2020-09-25 23:59:59'
    and collect_clue_type = 0
1301


select count(distinct clue_id)
from guazi_dw_dwd.dwd_com_recheck_task_ymd as a
where  dt = '${date_y_m_d}'
    and recheck_status in (5,6)
    and recheck_result <> 9
    and is_open_platform = 0
    and created_at between @startTime and @endTime
    and collect_clue_type = 0
1145