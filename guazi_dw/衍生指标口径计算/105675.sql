select avg((unix_timestamp(first_submit_time)-unix_timestamp(recheck_start_time))/60)
from realtime_dwd.dwd_recheck_task as a
where  recheck_start_time between '2020-09-25 00:00:00' and '2020-09-25 23:59:59'
    and first_submit_time >= recheck_start_time
    and recheck_status in (5,6)
    and recheck_result <> 9
    and recheck_before_audit = 1
    and order_type = 2
    and collect_clue_type = 0
21.33888888888889

select avg((unix_timestamp(first_submit_time)-unix_timestamp(recheck_start_time))/60)
fromguazi_dw_dwd.dwd_com_recheck_task_ymd as a
where   dt = '${date_y_m_d}'
    and recheck_start_time between @startTime and @endTime
    and first_submit_time >= recheck_start_time
    and recheck_status in (5,6)
    and recheck_result <> 9
    and recheck_before_audit = 1
    and order_type = 2
    and collect_clue_type = 0

    21.32777777777778