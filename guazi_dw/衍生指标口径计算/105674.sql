select avg((unix_timestamp(first_submit_time)-unix_timestamp(recheck_start_time))/60)
from realtime_dwd.dwd_recheck_task as a
where  recheck_start_time between '2020-09-25 00:00:00' and '2020-09-25 23:59:59'
    and first_submit_time >= recheck_start_time
    and recheck_status in (5,6)
    and recheck_result <> 9
    and recheck_before_audit = 1
    and order_type = 1
    and collect_clue_type = 0
245.68319746376818

select avg((unix_timestamp(first_submit_time)-unix_timestamp(recheck_start_time))/60)
from guazi_dw_dwd.dwd_com_recheck_task_ymd as a
where  dt = '${date_y_m_d}'
    and recheck_start_time between  @startTime and @endTime
    and first_submit_time >= recheck_start_time
    and recheck_status in (5,6)
    and recheck_result <> 9
    and recheck_before_audit = 1
    and order_type = 1
    and collect_clue_type = 0
38.52626543209879

离线与实时表数据存在不一致，因为在离线表中 限定了时间分区，所以first_submit_time只能是当天的时间，而事实表可以是recheck_start_time之后的任一天